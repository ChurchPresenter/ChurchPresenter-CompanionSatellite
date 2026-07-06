package companionsatellite

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStream
import java.net.Socket
import java.util.Base64

enum class CompanionConnectionStatus { DISCONNECTED, CONNECTING, CONNECTED, ERROR }

/** One button's state as last reported by Companion, in protocol-native form. */
data class CompanionButtonUpdate(
    val index: Int,
    val bitmapRgb: ByteArray? = null,
    val bitmapSize: Int = 0,
    val text: String = "",
    val color: String? = null,
    val textColor: String? = null,
    val pressed: Boolean = false,
    /** Parsed from `LOCATION="page/row/column"` (API >= 1.10.0) — null if Companion omits it. */
    val page: Int? = null
)

/**
 * Client for Bitfocus Companion's Satellite protocol (plain TCP, default port 16622,
 * line-based `COMMAND key=value key2="quoted value"` framing). Registers as a plain grid
 * surface (legacy `ADD-DEVICE` form — `KEYS_TOTAL`/`KEYS_PER_ROW`/`BITMAPS`, no
 * `LAYOUT_MANIFEST`), reports the bitmaps Companion streams for each button, and forwards
 * button presses back. Protocol confirmed against `bitfocus/companion-satellite` and
 * `bitfocus/companion` source.
 *
 * No UI-toolkit dependency by design — [onButtonUpdated] hands back raw RGB bytes so any
 * consumer (Compose, a CLI, a test) can decode them however it likes.
 */
class CompanionSatelliteClient(
    private val onStatusChanged: (CompanionConnectionStatus, String?) -> Unit,
    private val onButtonUpdated: (CompanionButtonUpdate) -> Unit,
    private val onButtonsReset: (count: Int) -> Unit,
    private val onBrightnessChanged: (percent: Int) -> Unit = {}
) {
    companion object {
        /** Companion's TCP layer times out an idle socket after 5s — ping comfortably under that. */
        private const val PING_INTERVAL_MS = 2000L
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var connectJob: Job? = null
    private var socket: Socket? = null
    private var writer: OutputStream? = null
    private val writeLock = Any()
    private var activeDeviceId: String = ""

    @Volatile private var currentStatus = CompanionConnectionStatus.DISCONNECTED

    fun connect(
        host: String,
        port: Int,
        deviceId: String,
        rows: Int,
        columns: Int,
        bitmapSize: Int,
        productName: String = "ChurchPresenter",
        reconnectDelayMs: Long = 2000L
    ) {
        disconnect()
        activeDeviceId = deviceId
        onButtonsReset(rows * columns)
        connectJob = scope.launch {
            connectLoop(host, port, deviceId, rows, columns, bitmapSize, productName, reconnectDelayMs)
        }
    }

    fun disconnect() {
        connectJob?.cancel()
        connectJob = null
        runCatching { socket?.close() }
        socket = null
        writer = null
        activeDeviceId = ""
        setStatus(CompanionConnectionStatus.DISCONNECTED, null)
    }

    fun dispose() {
        disconnect()
        scope.cancel()
    }

    /** Sends a down-then-up press for the button at [index], as a real key press would. */
    fun pressButton(index: Int) {
        val deviceId = activeDeviceId
        if (deviceId.isEmpty() || currentStatus != CompanionConnectionStatus.CONNECTED) return
        scope.launch {
            sendMessage("KEY-PRESS", deviceId, linkedMapOf("KEY" to index, "PRESSED" to true))
            delay(80)
            sendMessage("KEY-PRESS", deviceId, linkedMapOf("KEY" to index, "PRESSED" to false))
        }
    }

    /** Requests [times] relative page navigations (Companion's protocol has no "go to page N" — only
     * step forward/backward), paced so Companion has time to process each before the next. */
    fun changePage(forward: Boolean, times: Int = 1) {
        val deviceId = activeDeviceId
        if (deviceId.isEmpty() || currentStatus != CompanionConnectionStatus.CONNECTED || times <= 0) return
        scope.launch {
            repeat(times) {
                sendMessage("CHANGE-PAGE", deviceId, linkedMapOf("DIRECTION" to forward))
                delay(150)
            }
        }
    }

    private fun setStatus(status: CompanionConnectionStatus, error: String?) {
        currentStatus = status
        onStatusChanged(status, error)
    }

    private suspend fun connectLoop(
        host: String,
        port: Int,
        deviceId: String,
        rows: Int,
        columns: Int,
        bitmapSize: Int,
        productName: String,
        reconnectDelayMs: Long
    ) {
        while (scope.isActive) {
            setStatus(CompanionConnectionStatus.CONNECTING, null)
            try {
                Socket(host, port).use { s ->
                    socket = s
                    writer = s.getOutputStream()
                    val reader = BufferedReader(InputStreamReader(s.getInputStream(), Charsets.UTF_8))
                    // Companion closes the TCP socket after 5s of no traffic in either direction
                    // (net.Socket.setTimeout(5000) server-side) — ping well under that or Companion
                    // drops us and our reconnect loop kicks in, forever.
                    val pingJob = scope.launch {
                        while (isActive) {
                            delay(PING_INTERVAL_MS)
                            writeLine("PING\n")
                        }
                    }
                    try {
                        while (scope.isActive) {
                            val line = reader.readLine() ?: break
                            handleLine(line, deviceId, rows, columns, bitmapSize, productName)
                        }
                    } finally {
                        pingJob.cancel()
                    }
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                setStatus(CompanionConnectionStatus.ERROR, e.message ?: "Connection failed")
            }
            socket = null
            writer = null
            if (currentStatus != CompanionConnectionStatus.ERROR) setStatus(CompanionConnectionStatus.DISCONNECTED, null)
            if (!scope.isActive) break
            delay(reconnectDelayMs)
        }
    }

    private fun handleLine(line: String, deviceId: String, rows: Int, columns: Int, bitmapSize: Int, productName: String) {
        val trimmed = line.removeSuffix("\r")
        val spaceIndex = trimmed.indexOf(' ')
        val cmd = if (spaceIndex == -1) trimmed else trimmed.substring(0, spaceIndex)
        val body = if (spaceIndex == -1) "" else trimmed.substring(spaceIndex + 1)
        val params = parseLineParameters(body)

        when (cmd.uppercase()) {
            "PING" -> writeLine("PONG $body\n")
            "BEGIN" -> registerDevice(deviceId, rows, columns, bitmapSize, productName)
            "ADD-DEVICE" -> {
                if ("OK" in params) {
                    setStatus(CompanionConnectionStatus.CONNECTED, null)
                } else {
                    setStatus(CompanionConnectionStatus.ERROR, params["MESSAGE"] ?: "Device registration failed")
                }
            }
            "KEY-STATE" -> handleKeyState(params, bitmapSize)
            "KEYS-CLEAR" -> onButtonsReset(rows * columns)
            "BRIGHTNESS" -> params["VALUE"]?.toIntOrNull()?.let { onBrightnessChanged(it) }
            // PONG/REMOVE-DEVICE/DEVICE-CONFIG/CAPS need no action for a plain grid client.
            else -> {}
        }
    }

    private fun handleKeyState(params: Map<String, String>, bitmapSize: Int) {
        val keyIndex = params["KEY"]?.toIntOrNull() ?: return
        val bitmapRgb = params["BITMAP"]?.let { runCatching { Base64.getDecoder().decode(it) }.getOrNull() }
        val text = params["TEXT"]?.let { runCatching { String(Base64.getDecoder().decode(it)) }.getOrDefault("") } ?: ""
        val page = params["LOCATION"]?.substringBefore('/')?.toIntOrNull()
        onButtonUpdated(
            CompanionButtonUpdate(
                index = keyIndex,
                bitmapRgb = bitmapRgb,
                bitmapSize = bitmapSize,
                text = text,
                color = params["COLOR"],
                textColor = params["TEXTCOLOR"],
                pressed = params["PRESSED"] == "1",
                page = page
            )
        )
    }

    private fun registerDevice(deviceId: String, rows: Int, columns: Int, bitmapSize: Int, productName: String) {
        sendMessage(
            "ADD-DEVICE", deviceId, linkedMapOf(
                "PRODUCT_NAME" to productName,
                "KEYS_TOTAL" to rows * columns,
                "KEYS_PER_ROW" to columns,
                "BITMAPS" to bitmapSize,
                "COLORS" to true,
                "TEXT" to true,
                "TEXT_STYLE" to false,
                "BRIGHTNESS" to false,
                "VARIABLES" to Base64.getEncoder().encodeToString("[]".toByteArray())
            )
        )
    }

    private fun sendMessage(name: String, deviceId: String?, args: Map<String, Any>) {
        val line = buildString {
            append(name)
            if (deviceId != null) append(" DEVICEID=\"").append(deviceId).append('"')
            for ((key, value) in args) {
                append(' ').append(key).append('=')
                when (value) {
                    is Boolean -> append(if (value) "1" else "0")
                    is Number -> append(value.toString())
                    else -> append('"').append(value.toString()).append('"')
                }
            }
            append('\n')
        }
        writeLine(line)
    }

    private fun writeLine(line: String) {
        val out = writer ?: return
        synchronized(writeLock) {
            runCatching {
                out.write(line.toByteArray(Charsets.UTF_8))
                out.flush()
            }
        }
    }

    /** Parses `key=value key2="quoted value" boolFlag` tokens, splitting each on the first `=`
     * only (so base64 padding `=` inside a value isn't corrupted). Bare tokens map to `"true"`. */
    private fun parseLineParameters(line: String): Map<String, String> {
        val fragments = mutableListOf(StringBuilder())
        var inQuotes = false
        var i = 0
        while (i < line.length) {
            val c = line[i]
            when {
                c == '\\' && i + 1 < line.length -> {
                    fragments.last().append(line[i + 1])
                    i += 2
                }
                c == '"' -> {
                    inQuotes = !inQuotes
                    i++
                }
                c == ' ' && !inQuotes -> {
                    fragments.add(StringBuilder())
                    i++
                }
                else -> {
                    fragments.last().append(c)
                    i++
                }
            }
        }
        val result = mutableMapOf<String, String>()
        for (fragment in fragments) {
            val token = fragment.toString()
            if (token.isEmpty()) continue
            val eq = token.indexOf('=')
            if (eq == -1) result[token] = "true" else result[token.substring(0, eq)] = token.substring(eq + 1)
        }
        return result
    }
}
