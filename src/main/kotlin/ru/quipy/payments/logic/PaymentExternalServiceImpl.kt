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

        // Множество HTTP-статусов, при которых запрос можно безопасно повторить
        // Включает:
        //   429 - Too Many Requests (слишком много запросов, нужно повторить позже)
        //   500 - Internal Server Error (временная ошибка сервера)
        //   502 - Bad Gateway (проблемы с прокси/шлюзом)
        //   503 - Service Unavailable (сервис временно недоступен)
        //   504 - Gateway Timeout (таймаут шлюза)
        private val retryableHttpCodes = setOf(429, 500, 502, 503, 504)
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec

    private var rateLimiter: RateLimiter = SlidingWindowRateLimiter(10, Duration.ofSeconds(1))
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId, txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        // Бесконечный цикл для получения "окна" в системе ограничения параллельных запросов
        while (true) {
            val windowResponse = ongoingWindow.putIntoWindow()
            // Если успешно получили "окно" для выполнения запроса
            if (windowResponse is NonBlockingOngoingWindow.WindowResponse.Success) {
                break
            }
            // Проверка на превышение максимального времени ожидания
            if (System.currentTimeMillis() >= deadline) {
                logger.warn("[$accountName] Timeout waiting for parallel requests limit for payment $paymentId")
                paymentESService.update(paymentId) {
                    it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
                return  // Прекращаем выполнение при таймауте
            }
        }

// Формирование HTTP-запроса к внешней системе
        val request = Request.Builder()
            .url("http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            .post(emptyBody)  // POST-запрос с пустым телом
            .build()

// Параметры для повторных попыток
        val maxRetries = 1_000_000  // Максимальное число попыток (фактически бесконечность)
        var attempt = 0             // Счетчик попыток
        var success = false         // Флаг успешного выполнения
        var responseBody: ExternalSysResponse? = null  // Ответ внешней системы
        var lastException: Exception? = null  // Последняя ошибка

        try {
            // Основной цикл повторных попыток
            while (attempt < maxRetries && !success) {
                try {
                    // Ожидание разрешения от rate limiter
                    while (!rateLimiter.tick()) {
                        // Пустое тело цикла - просто ждем
                    }

                    // Проверка на превышение deadline с учетом среднего времени обработки
                    if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                        return
                    }

                    // Выполнение HTTP-запроса
                    client.newCall(request).execute().use { response ->
                        val code = response.code

                        // Обработка повторяемых ошибок (429, 500, 502, 503, 504)
                        if (code in retryableHttpCodes) {
                            throw TransientHttpException(code, "Received HTTP $code from server")
                        }

                        // Обработка неуспешных (но не повторяемых) ответов
                        if (code !in 200..299) {
                            logger.error("[$accountName] Non-retryable HTTP code $code for txId: $transactionId, payment: $paymentId")
                            responseBody = try {
                                mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }
                        }

                        // Обработка успешного ответа
                        responseBody = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        success = true  // Успешное завершение
                    }
                } catch (e: TransientHttpException) {
                    // Обработка повторяемых HTTP-ошибок
                    lastException = e
                    attempt++
                    if (attempt < maxRetries) {
                        logger.warn("[$accountName] Attempt #$attempt failed with code ${e.code} for payment $paymentId. Retrying...", e)
                    }
                } catch (e: SocketTimeoutException) {
                    // Обработка таймаутов соединения
                    lastException = e
                    attempt++
                    if (attempt < maxRetries) {
                        logger.warn("[$accountName] Attempt #$attempt SocketTimeout for payment $paymentId. Retrying...", e)
                    }
                } catch (e: Exception) {
                    // Обработка непредвиденных ошибок
                    lastException = e
                    logger.error("[$accountName] Non-retryable exception for txId: $transactionId, payment: $paymentId", e)
                    break
                }
            }

            // Обработка ситуации, когда все попытки исчерпаны
            if (!success) {
                when (lastException) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", lastException)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }
                    is TransientHttpException -> {
                        logger.error("[$accountName] Max retries exceeded with transient code ${lastException.code} for txId: $transactionId, payment: $paymentId", lastException)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Max retries exceeded for transient error ${lastException.code}.")
                        }
                    }
                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", lastException)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = lastException?.message)
                        }
                    }
                }
                return
            }

            // Логирование успешного выполнения
            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${responseBody?.result}, message: ${responseBody?.message}")

            // Обновление статуса платежа
            paymentESService.update(paymentId) {
                it.logProcessing(responseBody?.result ?: false, now(), transactionId, reason = responseBody?.message)
            }
        } finally {
            // Всегда освобождаем "окно" в системе ограничения параллельных запросов
            ongoingWindow.releaseWindow()
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
