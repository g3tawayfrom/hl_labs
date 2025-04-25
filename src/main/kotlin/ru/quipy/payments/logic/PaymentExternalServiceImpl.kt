package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

class TransientHttpException(val code: Int, message: String?) : Exception(message)

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        private val emptyBody = RequestBody.create(null, ByteArray(0))

        private val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec

    private var rateLimiter: RateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val parallelRequests = properties.parallelRequests

    private val executor: ExecutorService = Executors.newFixedThreadPool(parallelRequests)

    private val client = OkHttpClient.Builder()
        .callTimeout(Duration.ofMillis(1300L))
        .build()

    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId, txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        // Запускаем асинхронное выполнение блока кода через ExecutorService
        executor.submit {
            // Бесконечный цикл для попытки размещения запроса в "окно" обработки
            while (true) {
                val windowResponse = ongoingWindow.putIntoWindow()
                // Если успешно получили место в "окне" - выходим из цикла
                if (windowResponse is NonBlockingOngoingWindow.WindowResponse.Success) {
                    break
                }
                // Проверяем, не истекло ли время на обработку
                if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                    logger.warn("[$accountName] Parallel requests limit timeout for payment $paymentId. Aborting external call.")
                    // Обновляем статус платежа в сервисе
                    paymentESService.update(paymentId) {
                        it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                    }
                    // Выходим из submit-блока
                    return@submit
                }
            }

            // Создаем HTTP-запрос к внешнему сервису
            val request = Request.Builder()
                .url("http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                .post(emptyBody)
                .build()

            // Настройки повторных попыток (retry)
            var delay = 1000L // начальная задержка между попытками
            var maxRetries = 10 // максимальное количество попыток
            var attempt = 0 // счетчик попыток

            // Цикл повторных попыток
            while (attempt < maxRetries) {
                try {
                    // Ожидаем доступ в rate limiter
                    while (!rateLimiter.tick()) {
                        // Проверяем, не истекло ли время на обработку
                        if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                            throw SocketTimeoutException()
                        }
                    }

                    // Выполняем HTTP-запрос
                    client.newCall(request).execute().use { response ->
                        // Парсим ответ сервера
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            // Логируем ошибку парсинга
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }
                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                        // Если успешный ответ - обновляем статус и выходим
                        if (body.result) {
                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }
                            return@submit
                        }

                        // Обработка различных HTTP-кодов ответа
                        when (response.code) {
                            400, 401, 403, 404, 405 -> {
                                // Клиентские ошибки - бросаем исключение
                                throw RuntimeException("Client error code: ${response.code}")
                            }
                            500 -> delay = 0 // Серверные ошибки - повторяем сразу
                        }

                        // Увеличиваем счетчик попыток и делаем задержку
                        attempt++
                        Thread.sleep(delay)
                    }
                } catch (e: SocketTimeoutException) {
                    // Обработка таймаута
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                } catch (e: Exception) {
                    // Обработка других исключений
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                } finally {
                    // В любом случае освобождаем место в "окне" обработки
                    ongoingWindow.releaseWindow()
                }
            }
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
