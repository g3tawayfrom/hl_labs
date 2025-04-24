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


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    // RateLimiter с алгоритмом "скользящего окна":
    // - 10 запросов
    // - Окно длительностью 1 секунда
    // (Ограничивает частоту запросов к внешней системе)
    private var rateLimiter: RateLimiter = SlidingWindowRateLimiter(10, Duration.ofSeconds(1))
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()
    // Неблокирующее "окно" для контроля параллельных запросов:
    // - Реализует семафороподобное поведение
    // - Гарантирует не более parallelRequests одновременных запросов
    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        // Блок получения "разрешения" на выполнение запроса:
        // Цикл продолжается пока не получим место в "окне" параллельных запросов
        // или не истечет deadline
        while (true) {
            // Попытка занять "слот" для параллельного запроса
            val windowResponse = ongoingWindow.putIntoWindow()
            // Успешное получение слота
            if (windowResponse is NonBlockingOngoingWindow.WindowResponse.Success) {
                break
            }

            // Проверка дедлайна:
            // Если время вышло - логируем и прерываем выполнение
            if (System.currentTimeMillis() >= deadline) {
                logger.warn("[$accountName] Parallel requests limit timeout for payment $paymentId. Aborting external call.")
                paymentESService.update(paymentId) {
                    it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
                return
            }
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        try {
            // Ожидание разрешения от rate limiter'а:
            // tick() возвращает true когда можно выполнить запрос
            // в рамках ограничения RPS (Requests Per Second)
            while (!rateLimiter.tick()) {
                // Пустая операция - активное ожидание
                // На практике здесь можно добавить небольшую паузу
                Unit
            }
            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            ongoingWindow.releaseWindow()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()