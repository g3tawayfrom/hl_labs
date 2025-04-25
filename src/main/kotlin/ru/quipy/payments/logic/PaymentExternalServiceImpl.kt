package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.*


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    // Компаньон-объект содержит общие для всех экземпляров класса элементы
    companion object {
        // Логгер для записи событий
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        // Объект для сериализации/десериализации JSON с поддержкой Kotlin модуля
        val mapper = ObjectMapper().registerKotlinModule()
    }

    // Конфигурационные параметры адаптера
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    // Настройка HTTP-клиента
    private val client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofMillis(requestAverageProcessingTime.toMillis() * 2))
        .build()

    // Семафор для ограничения количества одновременных запросов
    private val semaphore = Semaphore(parallelRequests)

    // Rate limiter для ограничения количества запросов в секунду
    private val rateLimiter = RateLimiter.of(
        "paymentRateLimiter-$accountName",
        RateLimiterConfig.custom()
            .limitRefreshPeriod(Duration.ofSeconds(1))
            .limitForPeriod(rateLimitPerSec)
            .timeoutDuration(Duration.ofSeconds(5))
            .build()
    )

    // Пул потоков для выполнения и планирования задач
    private val executor: ExecutorService = Executors.newFixedThreadPool(parallelRequests)
    private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(parallelRequests)

    // Основной метод для асинхронного выполнения платежа
    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        // Проверка, не превышен ли deadline для выполнения платежа
        if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
            logger.warn("[$accountName] PaymentId: $paymentId exceeds the deadline.")

            // Обновление статуса платежа в случае превышения deadline
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), UUID.randomUUID(), reason = "Request timeout.")
            }

            return
        }

        // Попытка занять слот семафора
        if (!semaphore.tryAcquire()) {
            logger.warn("[$accountName] Semaphore unavailable for paymentId: $paymentId. Retrying later.")

            // Планирование повторной попытки через 100 мс
            scheduler.schedule({
                performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
            }, 100, TimeUnit.MILLISECONDS)

            return
        }

        // Проверка rate limiter'а
        if (!rateLimiter.acquirePermission()) {
            logger.warn("[$accountName] Rate limiter unavailable for paymentId: $paymentId. Retrying later.")

            // Освобождение семафора, так как запрос не будет выполняться
            semaphore.release()

            // Планирование повторной попытки через 100 мс
            scheduler.schedule({
                performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
            }, 100, TimeUnit.MILLISECONDS)

            return
        }

        // Генерация уникального ID транзакции
        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Логирование факта отправки платежа (обязательно для сервиса тестирования)
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        // Формирование HTTP-запроса к внешней системе
        val request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        // Повторная проверка deadline (на случай, если прошло время)
        if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
            logger.warn("[$accountName] PaymentId: $paymentId exceeds the deadline.")

            // Обновление статуса платежа
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), UUID.randomUUID(), reason = "Request timeout.")
            }

            semaphore.release()
            return
        }

        // Асинхронная отправка запроса
        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                semaphore.release()

                // Парсинг ответа от внешней системы
                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                    ExternalSysResponse(
                        transactionId.toString(),
                        paymentId.toString(),
                        false,
                        e.message
                    )
                }

                // Коды ответов, при которых можно повторить запрос
                val retryableResponseCodes = setOf(200, 400, 401, 403, 404, 405)

                // Если ответ не успешный - планируем повторный запрос
                if (response.statusCode() !in retryableResponseCodes || !body.result) {
                    val delayDuration = response.headers().firstValue("Retry-After")
                        .map { Duration.parse(it) }
                        .orElse(Duration.ofMillis(100))

                    scheduler.schedule({
                        performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
                    }, delayDuration.toMillis(), TimeUnit.MILLISECONDS)

                    return@thenApply null
                }

                // Логирование успешной обработки платежа
                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Обновление статуса платежа в БД (обязательно для всех исходов)
                paymentESService.update(paymentId) {
                    it.logProcessing(true, now(), transactionId, reason = body.message)
                }
            }
            .exceptionally { e ->
                semaphore.release()

                // Обработка ошибок при выполнении запроса
                logger.error(
                    "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                    e
                )

                // Обновление статуса платежа в случае ошибки
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }

                // Планирование повторной попытки через 200 мс
                scheduler.schedule({
                    performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
                }, 200, TimeUnit.MILLISECONDS)

                return@exceptionally null
            }
    }

    // Методы для получения информации о платежной системе
    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()