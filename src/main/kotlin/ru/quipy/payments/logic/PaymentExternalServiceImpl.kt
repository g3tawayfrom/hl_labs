package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resume

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val tokenBucketRateLimiter = TokenBucketRateLimiter(
        rateLimitPerSec,
        rateLimitPerSec,
        Duration.ofSeconds(1).toMillis(),
        TimeUnit.MILLISECONDS
    )
    private val processingTimeMillis = 50_000L
    private val client = OkHttpClient.Builder()
        .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
        .connectionPool(ConnectionPool(350, 5, TimeUnit.MINUTES))
        .dispatcher(Dispatcher().apply {
            maxRequests = 1000
            maxRequestsPerHost = 1000
        })
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(60, TimeUnit.SECONDS)
        .writeTimeout(60, TimeUnit.SECONDS)
        .callTimeout(requestAverageProcessingTime.toMillis() * 2, TimeUnit.MILLISECONDS)
        .build()

    private val coroutineScope = CoroutineScope(Dispatchers.Default)
    private val semaphore = Semaphore(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        coroutineScope.launch {
            logger.warn("[$accountName] Submitting payment request for payment $paymentId")

            val transactionId = UUID.randomUUID()
            logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            val request = Request.Builder()
                .url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                .post(RequestBody.create(null, ByteArray(0)))
                .build()

            try {
                withTimeout(processingTimeMillis) {
                    retryWithExponentialBackOff {
                        processRequest(request, paymentId, transactionId)
                    }
                }
            } catch (e: Exception) {
                handlePaymentException(e, paymentId, transactionId)
            }
        }
    }

    private fun handlePaymentException(e: Exception, paymentId: UUID, transactionId: UUID) {
        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = e.message)
        }
    }

    private suspend fun processRequest(request: Request, paymentId: UUID, transactionId: UUID): Boolean {
        val semaphoreStartTime = now()
        semaphore.withPermit {
            val semaphoreTime = now() - semaphoreStartTime
            val rateLimiterLeftTime = processingTimeMillis - semaphoreTime
            if (!tokenBucketRateLimiter.tryTick(rateLimiterLeftTime, TimeUnit.MILLISECONDS)) {
                throw Exception("Not enough time left for payment processing")
            }

            return suspendCancellableCoroutine { continuation ->
                client.newCall(request).enqueue(object : Callback {
                    override fun onFailure(call: Call, e: IOException) {
                        logger.error("[$accountName] Request failed: txId=$transactionId, payment=$paymentId", e)
                        continuation.resume(false)
                    }

                    override fun onResponse(call: Call, response: Response) {
                        response.use {
                            val body = try {
                                mapper.readValue(it.body?.string(), ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }

                            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }

                            continuation.resume(body.result)
                        }
                    }
                })
            }
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName

    private suspend fun retryWithExponentialBackOff(
        maxAttempts: Int = 3,
        initialDelay: Long = 2000L,
        block: suspend () -> Boolean
    ) {
        var delayTime = initialDelay
        repeat(maxAttempts) {
            if (block()) return
            delay(delayTime)
            delayTime *= 2
        }
    }
}

fun now() = System.currentTimeMillis()