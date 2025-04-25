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

class TransientHttpException(val code: Int, message: String?) : Exception(message)

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        private val emptyBody = RequestBody.create(null, ByteArray(0))

        private val mapper = ObjectMapper().registerKotlinModule()

        private val retryableHttpCodes = setOf(429, 500, 502, 503, 504)
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec

    private var rateLimiter: RateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val parallelRequests = properties.parallelRequests

    // Объявление приватного неизменяемого (val) свойства client
    private val client = OkHttpClient.Builder()  // Создание билдера для конфигурации OkHttpClient
        .callTimeout(Duration.ofMillis(1300L))   // Установка общего таймаута для всего вызова (запрос + обработка ответа) в 1300 миллисекунд
        .build()                                // Создание экземпляра OkHttpClient с заданными настройками

    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId, txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        while (true) {
            val windowResponse = ongoingWindow.putIntoWindow()
            if (windowResponse is NonBlockingOngoingWindow.WindowResponse.Success) {
                break
            }
            if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                logger.warn("[$accountName] Parallel requests limit timeout for payment $paymentId. Aborting external call.")
                paymentESService.update(paymentId) {
                    it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
                return
            }
        }

        val request = Request.Builder()
            .url("http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            .post(emptyBody)
            .build()

        // Начальные параметры для повторных попыток
        var delay = 1000L  // начальная задержка между попытками (1 секунда)
        var maxRetries = 10 // максимальное количество попыток
        var attempt = 0     // счетчик текущей попытки

        // Цикл повторных попыток
        while (attempt < maxRetries) {
            try {
                // Ожидание разрешения от rateLimiter (ограничителя частоты запросов)
                while (!rateLimiter.tick()) {
                    // Проверка, не истекло ли время deadline (крайний срок выполнения)
                    if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                        throw SocketTimeoutException() // Бросаем исключение, если время вышло
                    }
                }

                // Выполнение HTTP-запроса
                client.newCall(request).execute().use { response ->
                    // Парсинг тела ответа
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        // Логирование ошибки парсинга
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    // Логирование результата обработки платежа
                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    // Если платеж успешен - обновляем статус и выходим
                    if (body.result) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                        return
                    }

                    // Обработка кодов ответа
                    when (response.code) {
                        // Коды клиентских ошибок - бросаем исключение
                        400, 401, 403, 404, 405 -> {
                            throw RuntimeException("Client error code: ${response.code}")
                        }
                        // Код серверной ошибки - сбрасываем задержку для немедленной повторной попытки
                        500 -> delay = 0
                    }

                    // Увеличиваем счетчик попыток и делаем паузу перед следующей попыткой
                    attempt++
                    Thread.sleep(delay)
                }
            } catch (e: SocketTimeoutException) {
                // Обработка таймаута соединения
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
                // Освобождение ресурса (вероятно, семафора или чего-то подобного)
                ongoingWindow.releaseWindow()
            }
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
