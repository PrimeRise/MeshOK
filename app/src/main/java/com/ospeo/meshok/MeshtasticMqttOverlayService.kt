package com.ospeo.meshok

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Intent
import android.graphics.Color
import android.graphics.PixelFormat
import android.graphics.drawable.GradientDrawable
import android.net.Uri
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.os.VibrationEffect
import android.os.Vibrator
import android.os.VibratorManager
import android.text.SpannableString
import android.text.style.AbsoluteSizeSpan
import android.text.style.BackgroundColorSpan
import android.text.style.ForegroundColorSpan
import android.text.style.UnderlineSpan
import android.view.Gravity
import android.view.MotionEvent
import android.view.View
import android.view.ViewGroup
import android.view.WindowManager
import android.widget.*
import androidx.core.app.NotificationCompat
import org.eclipse.paho.client.mqttv3.*
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.json.JSONArray
import org.json.JSONObject
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.TimeUnit
import okhttp3.*

data class CachedMessage(
    val mergeHash: String, val displayName: String, val nodeId: String, val text: String,
    val ch: String, val region: String, val rssi: Int, val snr: Double, val hops: Int,
    val time: String, val raw: String, val mapUrl: String?, var source: String,
    val sortTime: Long, val replyNode: String = ""
)

class MeshtasticMqttOverlayService : Service() {

    private lateinit var wm: WindowManager
    private lateinit var overlayView: View
    private lateinit var messagesContainer: LinearLayout
    private lateinit var scrollView: ScrollView
    private lateinit var scrollDownButton: Button
    private lateinit var topicText: TextView
    private lateinit var statusText: TextView
    private var mqttClient: MqttClient? = null
    private var wsClient: WebSocket? = null
    private val handler = Handler(Looper.getMainLooper())
    private var vibrator: Vibrator? = null
    private var lp: WindowManager.LayoutParams? = null
    private var isUserScrolling = false
    private var initialMessagesLoaded = false
    private var reconnectAttempts = 0
    private var settingsOverlay: View? = null
    private var miniOverlay: View? = null
    private var sendOverlay: View? = null
    private var logOverlay: View? = null
    private var pingRunnable: Runnable? = null
    private var wsReconnectRunnable: Runnable? = null

    private var brokerUrl = "tcp://mqtt.meshtastic.org:1883"
    private var username = "meshdev"
    private var password = "large4cats"
    private var currentRegion = "RU/MSK"
    private var customRegion = ""
    private var filterNodeId = ""
    private var showTelemetry = true
    private var showPosition = true
    private var showNodes = false
    private var showMqtt = true
    private var showOnemesh = true
    private var timezoneOffset = "+3"
    private var topics = arrayOf("msh/$currentRegion/2/json/#")
    private var onemeshUrl = "wss://ospeo.duckdns.org:9443"
    private var myNodeId = ""
    private var sendDisclaimerShown = false

    private val nodeDb = mutableMapOf<String, Triple<String, String, Long>>()
    private val timeFormat = SimpleDateFormat("HH:mm:ss", Locale.getDefault())
    private val isoFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US)
    private val rawLogs = mutableListOf<String>()
    private val seenHashes = mutableSetOf<String>()
    private val seenDedup = mutableSetOf<String>()
    private val msgViewMap = mutableMapOf<String, TextView>()

    companion object { private const val MAX_MESSAGES = 200 }

    override fun onBind(i: Intent?): IBinder? = null

    override fun onCreate() {
        super.onCreate()
        vibrator = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) { val vm = getSystemService(VIBRATOR_MANAGER_SERVICE) as VibratorManager; vm.defaultVibrator } else { @Suppress("DEPRECATION") getSystemService(VIBRATOR_SERVICE) as Vibrator }
        val p = getSharedPreferences("meshok", MODE_PRIVATE)
        brokerUrl = p.getString("broker", brokerUrl) ?: brokerUrl; username = p.getString("user", username) ?: username; password = p.getString("pass", password) ?: password
        currentRegion = p.getString("region", "RU/MSK") ?: "RU/MSK"; customRegion = p.getString("custom_region", "") ?: ""; filterNodeId = p.getString("filter", "") ?: ""
        showTelemetry = p.getBoolean("showTel", true); showPosition = p.getBoolean("showPos", true); showNodes = p.getBoolean("showNodes", false)
        showMqtt = p.getBoolean("showMqtt", true); showOnemesh = p.getBoolean("showOnemesh", true)
        timezoneOffset = p.getString("timezone", "+3") ?: "+3"; onemeshUrl = p.getString("onemeshUrl", onemeshUrl) ?: onemeshUrl
        myNodeId = p.getString("myNodeId", "") ?: ""; sendDisclaimerShown = p.getBoolean("sendDisc", false)
        loadNodeDb(); setTopics(); startForegroundNotification(); setupOverlayWindow()
        if (showMqtt) connectMqtt(); startPing()
        if (showOnemesh) connectWebSocket()
        debugLog("Сервис запущен")
    }

    private fun vibrate() {
        try { vibrator?.let { if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) it.vibrate(VibrationEffect.createOneShot(1, VibrationEffect.DEFAULT_AMPLITUDE)) else @Suppress("DEPRECATION") it.vibrate(1) } } catch (_: Exception) {}
    }

    private fun saveNodeDb() {
        try { val json = JSONObject(); for ((id, data) in nodeDb) { val entry = JSONObject(); entry.put("sn", data.first); entry.put("ln", data.second); entry.put("ts", data.third); json.put(id, entry) }; getSharedPreferences("meshok", MODE_PRIVATE).edit().putString("nodeDb", json.toString()).apply() } catch (e: Exception) { debugLog("Ошибка сохранения NodeDB: ${e.message}") }
    }

    private fun loadNodeDb() {
        try { val saved = getSharedPreferences("meshok", MODE_PRIVATE).getString("nodeDb", null); if (saved != null) { val json = JSONObject(saved); val keys = json.keys(); while (keys.hasNext()) { val id = keys.next(); val entry = json.getJSONObject(id); nodeDb[id] = Triple(entry.optString("sn",""), entry.optString("ln",""), entry.optLong("ts",0)) }; updateCounters() } } catch (e: Exception) { debugLog("Ошибка загрузки NodeDB: ${e.message}") }
    }

    private fun hash(sender: String, ch: String, text: String): String { 
        try { val md = MessageDigest.getInstance("MD5"); md.update("$sender|$ch|$text".toByteArray()); return md.digest().joinToString("") { String.format("%02x", it) } } catch (e: Exception) { return UUID.randomUUID().toString() }
    }

    private fun setTopics() { val region = if (customRegion.isNotBlank()) customRegion else currentRegion; topics = arrayOf("msh/$region/2/json/#"); if (::topicText.isInitialized) topicText.text = "msh/$region" }
    private fun setStatus(msg: String, color: Int = Color.parseColor("#AAFFAA")) { if (::statusText.isInitialized) handler.post { statusText.text = msg; statusText.setTextColor(color) } }
    private fun flashStatus(msg: String) { setStatus(msg, Color.parseColor("#AAAAAA")); handler.postDelayed({ if (statusText.text == msg) setStatus("Подключено") }, 2000) }
    private fun startPing() { pingRunnable = object : Runnable { override fun run() { try { mqttClient?.publish("msh/ping", ByteArray(0), 0, false) } catch (_: Exception) {}; handler.postDelayed(this, 60000) } }; handler.postDelayed(pingRunnable!!, 60000) }
    private fun debugLog(msg: String) { val timestamp = timeFormat.format(Date()); rawLogs.add("[$timestamp] $msg"); if (rawLogs.size > 500) rawLogs.removeAt(0); flashStatus(msg) }

    private fun scheduleWsReconnect() {
        wsReconnectRunnable?.let { handler.removeCallbacks(it) }
        wsReconnectRunnable = Runnable { debugLog("WSS: переподключение..."); if (showOnemesh) connectWebSocket() }
        handler.postDelayed(wsReconnectRunnable!!, 5000)
    }

    private fun connectWebSocket() {
        try {
            val client = OkHttpClient.Builder().readTimeout(0, TimeUnit.MILLISECONDS).build()
            debugLog("WSS: подключение к $onemeshUrl...")
            wsClient = client.newWebSocket(Request.Builder().url(onemeshUrl).build(), object : WebSocketListener() {
                override fun onOpen(webSocket: WebSocket, response: Response) { debugLog("WSS: подключено"); initialMessagesLoaded = false }
                override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) { debugLog("WSS ошибка: ${t.message}"); scheduleWsReconnect() }
                override fun onClosed(webSocket: WebSocket, code: Int, reason: String) { debugLog("WSS закрыт: $code $reason"); scheduleWsReconnect() }
                override fun onMessage(webSocket: WebSocket, text: String) {
                    try {
                        val json = JSONObject(text)
                        when (json.optString("type")) {
                            "nodes", "node_update" -> {
                                val nodes = json.optJSONObject("data") ?: return
                                val now = System.currentTimeMillis()
                                val iter = nodes.keys()
                                while (iter.hasNext()) { val id = iter.next(); val entry = nodes.getJSONObject(id); val sn = entry.optString("sn", ""); val ln = entry.optString("ln", ""); nodeDb[id] = Triple(if (sn.isNotEmpty()) sn else id.take(5), ln, now) }
                                saveNodeDb(); handler.post { updateCounters() }
                            }
                            "messages" -> {
                                val msgs = json.optJSONArray("data") ?: return
                                val batch = mutableListOf<CachedMessage>()
                                for (i in 0 until msgs.length()) {
                                    try {
                                        val m = msgs.getJSONObject(i)
                                        val fromNum = m.optLong("from", 0)
                                        val from = if (fromNum > 0) "!" + String.format("%08x", fromNum) else "?"
                                        if (from == "?") continue
                                        val txt = m.optString("text", ""); if (txt.isBlank()) continue
                                        val replyNode = m.optString("reply_to_node", "").trim()
                                        val ch = m.optString("channel_id", "CH?").let { when(it){"LongFast"->"LF";"MediumFast"->"MF";"MediumSlow"->"MS";"ShortSlow"->"SS";"LongSlow"->"LS";else->it.take(4)} }
                                        val createdAt = m.optString("created_at", "")
                                        val sortTime = try { isoFormat.parse(createdAt)?.time ?: System.currentTimeMillis() } catch (_: Exception) { System.currentTimeMillis() }
                                        val tzOffset = (timezoneOffset.replace("+", "").toIntOrNull() ?: 0) * 3600 * 1000L
                                        val timeStr = if (createdAt.isNotEmpty()) timeFormat.format(Date(sortTime + tzOffset)) else ""
                                        val rootTopic = m.optString("root_topic", ""); val city = rootTopic.split("/").getOrNull(2) ?: ""; val regionDisplay = if (city.isNotEmpty()) " $city" else ""
                                        val selectedCity = if (currentRegion.startsWith("RU/")) currentRegion.substring(3) else ""
                                        if (selectedCity.isNotEmpty() && selectedCity != "+" && city.isNotEmpty() && !city.equals(selectedCity, true)) continue
                                        val rssi = m.optInt("rx_rssi", Int.MIN_VALUE); val snr = m.optDouble("rx_snr", Double.MIN_VALUE); val hops = m.optInt("hop_limit", -1)
                                        val fromName = m.optString("from_name", "")
                                        if (fromName.isNotEmpty()) nodeDb[from] = Triple(fromName.take(4), fromName, System.currentTimeMillis())
                                        val node = nodeDb[from]; val dbShort = node?.first ?: ""; val dbLong = node?.second ?: ""
                                        val displayName = if (dbShort.isNotEmpty()) dbShort else if (dbLong.isNotEmpty()) dbLong.take(16) else from
                                        val dedupKey = "$from|$ch|$txt|$createdAt"
                                        if (dedupKey in seenDedup) continue
                                        seenDedup.add(dedupKey); val mergeHash = hash(from, ch, txt)
                                        batch.add(CachedMessage(mergeHash, displayName, from, txt, ch, regionDisplay, rssi, snr, hops, timeStr, txt, null, "ONE", sortTime, replyNode))
                                    } catch (e: Exception) { debugLog("WSS ошибка разбора: ${e.message}") }
                                }
                                if (batch.isNotEmpty()) { handler.post { batch.forEach { addOrUpdateMessage(it) }; if (!initialMessagesLoaded) { initialMessagesLoaded = true; debugLog("WSS: загружено ${batch.size} сообщ") } } }
                            }
                        }
                    } catch (e: Exception) { debugLog("WSS ошибка JSON: ${e.message}") }
                }
            })
        } catch (e: Exception) { debugLog("WSS ошибка создания: ${e.message}"); scheduleWsReconnect() }
    }

    private fun updateCounters() { 
        try { val named = nodeDb.count { it.value.first.isNotEmpty() }; val total = nodeDb.size; val online = nodeDb.count { System.currentTimeMillis() - it.value.third < 12 * 3600 * 1000 }; if (::topicText.isInitialized) topicText.text = "👤$named 🌐$total 🟢$online" } catch (e: Exception) { debugLog("Ошибка счетчиков: ${e.message}") }
    }

    private fun addOrUpdateMessage(cached: CachedMessage) {
        try {
            if (cached.mergeHash in seenHashes) { val existing = msgViewMap[cached.mergeHash]; if (existing != null) { val existingText = existing.text.toString(); val currentSource = when { existingText.startsWith("MQTT") -> "MQTT"; existingText.startsWith("ONE") -> "ONE"; else -> "" }; if (currentSource != cached.source) { val tv = createMessageView(cached); existing.text = tv.text } }; return }
            seenHashes.add(cached.mergeHash)
            val tv = createMessageView(cached); tv.tag = cached.mergeHash; msgViewMap[cached.mergeHash] = tv
            messagesContainer.addView(tv)
            while (messagesContainer.childCount > MAX_MESSAGES) { val old = messagesContainer.getChildAt(0) as? TextView; old?.let { msgViewMap.remove(it.tag as? String) }; messagesContainer.removeViewAt(0) }
            if (!isUserScrolling) scrollView.post { scrollView.fullScroll(View.FOCUS_DOWN) }
        } catch (e: Exception) { debugLog("Ошибка добавления: ${e.message}") }
    }

    private fun createMessageView(msg: CachedMessage): TextView {
        val sig = if (msg.rssi != Int.MIN_VALUE) " ${msg.rssi}dBm" else ""; val hop = if (msg.hops >= 0) " 🦘${msg.hops}" else ""
        val tm = if (msg.time.isNotEmpty()) " ${msg.time}" else ""; val regionStr = msg.region
        val timeColor = if (msg.source == "MQTT") Color.parseColor("#8866CCFF") else Color.parseColor("#8800CCCC")
        val node = nodeDb[msg.nodeId]; val dbSn = node?.first ?: ""; val dbLn = node?.second ?: ""
        val bestName = if (dbSn.isNotEmpty()) dbSn else if (dbLn.isNotEmpty()) dbLn.take(16) else msg.displayName
        val showName = if (bestName.startsWith("!") && bestName.length >= 9) bestName else bestName
        val rn = msg.replyNode; val replyStr = if (rn.isNotEmpty() && rn.length < 10) " ➡ $rn" else ""
        val full = "$tm [${msg.ch}]$regionStr $showName$replyStr$sig$hop: ${msg.text}"
        return TextView(this).apply {
            textSize = 12f; setTextColor(Color.WHITE); setLinkTextColor(Color.parseColor("#66CCFF")); setPadding(4,4,4,4); maxLines = 10
            setOnLongClickListener { (getSystemService(CLIPBOARD_SERVICE) as ClipboardManager).setPrimaryClip(ClipData.newPlainText("Msg", "$full\n${msg.raw}")); flashStatus("Скопировано"); true }
            setOnClickListener { if (msg.mapUrl != null) try { startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(msg.mapUrl)).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)) } catch (_: Exception) {} }
            val ss = SpannableString(full)
            if (msg.time.isNotEmpty()) {
                val timeIdx = full.indexOf(msg.time)
                    val colonIdx = full.indexOf(":", timeIdx + 3)
                    if (colonIdx > 0) ss.setSpan(AbsoluteSizeSpan(9, true), colonIdx, timeIdx + msg.time.length, 0)
                if (timeIdx >= 0) ss.setSpan(BackgroundColorSpan(if (msg.source == "MQTT") Color.parseColor("#448866FF") else Color.parseColor("#4400CCCC")), timeIdx, timeIdx + msg.time.length, 0)
            }
            val chColor = when(msg.ch) { "LF" -> Color.parseColor("#FF8800"); "MF" -> Color.parseColor("#00CC00"); "SF" -> Color.parseColor("#00CCCC"); "FLOO" -> Color.parseColor("#CC00CC"); "LS" -> Color.parseColor("#CC8800"); "MS" -> Color.parseColor("#0088CC"); "SS" -> Color.parseColor("#CC4444"); else -> Color.WHITE }
            val chStart = full.indexOf("[") + 1; val chEnd = full.indexOf("]")
            if (chStart > 0 && chEnd > chStart) { ss.setSpan(ForegroundColorSpan(chColor), chStart, chEnd, 0); ss.setSpan(AbsoluteSizeSpan(10, true), chStart, chEnd, 0) }
            if (msg.region.isNotEmpty()) { val rIdx = full.indexOf(msg.region); if (rIdx >= 0) { ss.setSpan(AbsoluteSizeSpan(10, true), rIdx, rIdx + msg.region.length, 0); ss.setSpan(ForegroundColorSpan(Color.parseColor("#888888")), rIdx, rIdx + msg.region.length, 0) } }
            if (msg.rssi != Int.MIN_VALUE) { val sigColor = if (msg.rssi >= -100) Color.parseColor("#00CC00") else Color.parseColor("#FF8800"); val sigIdx = full.indexOf("${msg.rssi}dBm"); if (sigIdx >= 0) ss.setSpan(ForegroundColorSpan(sigColor), sigIdx, sigIdx + "${msg.rssi}dBm".length, 0); ss.setSpan(AbsoluteSizeSpan(10, true), sigIdx, sigIdx + "${msg.rssi}dBm".length, 0) }
            val nameIdx = full.indexOf(showName)
            if (nameIdx >= 0) {
                if (showName.startsWith("!") && showName.length >= 9) {
                    val nS = nameIdx; val nM = nameIdx + 5; val nE = nameIdx + showName.length
                    ss.setSpan(AbsoluteSizeSpan(10, true), nS, nM, 0); ss.setSpan(ForegroundColorSpan(Color.parseColor("#88FFFFFF")), nS, nM, 0)
                    ss.setSpan(AbsoluteSizeSpan(12, true), nM, nE, 0); ss.setSpan(ForegroundColorSpan(Color.WHITE), nM, nE, 0)
                    ss.setSpan(BackgroundColorSpan(Color.parseColor("#44FF6600")), nS, nE, 0)
                } else {
                    ss.setSpan(BackgroundColorSpan(colorFromName(showName)), nameIdx, nameIdx + showName.length, 0)
                }
            }
            if (rn.isNotEmpty() && rn.length < 10) { val replyIdx = full.indexOf("➡ $rn"); if (replyIdx >= 0) ss.setSpan(ForegroundColorSpan(Color.parseColor("#FFCC00")), replyIdx, replyIdx + rn.length + 2, 0) }
            if (msg.mapUrl != null) { val textIdx = full.indexOf(msg.text); if (textIdx >= 0) ss.setSpan(UnderlineSpan(), textIdx, textIdx + msg.text.length, 0) }
            setText(ss); movementMethod = android.text.method.LinkMovementMethod.getInstance()
        }
    }

    private fun showLogPopup() {
        vibrate()
        if (logOverlay != null) { try { wm.removeView(logOverlay) } catch (_: Exception) {}; logOverlay = null; return }
        val popup = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL; setPadding(16, 16, 16, 16) }
        stylePopupBg(popup)
        popup.addView(TextView(this).apply { text = "📋 Логи (долгое нажатие для копирования)"; textSize = 14f; setTextColor(Color.parseColor("#FFA500")); setPadding(0, 0, 0, 8) })
        val logScroll = ScrollView(this).apply { layoutParams = LinearLayout.LayoutParams(800, 400) }
        val logText = TextView(this).apply { text = rawLogs.joinToString("\n"); textSize = 10f; setTextColor(Color.parseColor("#CCCCCC")); setPadding(8, 8, 8, 8); setBackgroundColor(Color.parseColor("#22223344")); setTextIsSelectable(true); setOnLongClickListener { vibrate(); (getSystemService(CLIPBOARD_SERVICE) as ClipboardManager).setPrimaryClip(ClipData.newPlainText("MeshOK Logs", rawLogs.joinToString("\n"))); flashStatus("Логи скопированы (${rawLogs.size} записей)"); true } }
        logScroll.addView(logText); popup.addView(logScroll); logScroll.post { logScroll.fullScroll(View.FOCUS_DOWN) }
        val btns = LinearLayout(this).apply { orientation = LinearLayout.HORIZONTAL; setPadding(0, 12, 0, 0); gravity = Gravity.CENTER }
        val closeBtn = Button(this).apply { text = "ЗАКРЫТЬ"; setTextColor(Color.WHITE); setOnClickListener { vibrate(); try { wm.removeView(popup) } catch (_: Exception) {}; logOverlay = null } }
        styleButton(closeBtn); btns.addView(closeBtn); popup.addView(btns); logOverlay = popup
        wm.addView(popup, WindowManager.LayoutParams(WindowManager.LayoutParams.WRAP_CONTENT, WindowManager.LayoutParams.WRAP_CONTENT, if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY else WindowManager.LayoutParams.TYPE_PHONE, WindowManager.LayoutParams.FLAG_NOT_TOUCH_MODAL or WindowManager.LayoutParams.FLAG_WATCH_OUTSIDE_TOUCH, PixelFormat.TRANSLUCENT).apply { gravity = Gravity.CENTER })
    }

    private fun showSendPopup() {
        vibrate(); if (!sendDisclaimerShown) { showDisclaimerPopup(); return }; if (myNodeId.isEmpty()) { showNodeIdPopup(); return }; if (sendOverlay != null) return
        val popup = LinearLayout(this).apply { orientation = LinearLayout.HORIZONTAL; setPadding(12, 8, 12, 8) }; stylePopupBg(popup)
        val input = EditText(this).apply { setTextColor(Color.WHITE); hint = "Сообщение..."; layoutParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f) }; styleInput(input); popup.addView(input)
        val sendBtn = Button(this).apply { text = "▶"; setTextColor(Color.WHITE); setOnClickListener { vibrate(); val text = input.text.toString().trim(); if (text.isNotBlank()) { try { val json = JSONObject().apply { put("from", myNodeId.replace("!", "").toLongOrNull() ?: 0); put("type", "sendtext"); put("payload", text) }; mqttClient?.publish("msh/$currentRegion/2/json/mqtt/" + myNodeId, json.toString().toByteArray(Charsets.UTF_8), 0, false); flashStatus("Отправлено"); debugLog("Отправлено: $text") } catch (e: Exception) { debugLog("Ошибка отправки: ${e.message}") } }; try { wm.removeView(popup) } catch (_: Exception) {}; sendOverlay = null } }; styleButton(sendBtn); popup.addView(sendBtn)
        sendOverlay = popup; wm.addView(popup, WindowManager.LayoutParams(WindowManager.LayoutParams.WRAP_CONTENT, WindowManager.LayoutParams.WRAP_CONTENT, if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY else WindowManager.LayoutParams.TYPE_PHONE, WindowManager.LayoutParams.FLAG_NOT_TOUCH_MODAL or WindowManager.LayoutParams.FLAG_WATCH_OUTSIDE_TOUCH, PixelFormat.TRANSLUCENT).apply { gravity = Gravity.BOTTOM or Gravity.CENTER; y = 100 })
    }

    private fun showDisclaimerPopup() {
        val popup = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL; setPadding(24, 24, 24, 24) }; stylePopupBg(popup)
        popup.addView(TextView(this).apply { text = "⚠ Внимание"; textSize = 16f; setTextColor(Color.parseColor("#FFA500")); setPadding(0, 0, 0, 8) })
        popup.addView(TextView(this).apply { text = "Убедитесь, что:\n- Нода запущена\n- Устройство подключено к сети интернет\n- Включён downlink"; textSize = 13f; setTextColor(Color.WHITE); setPadding(0, 0, 0, 16) })
        val btns = LinearLayout(this).apply { orientation = LinearLayout.HORIZONTAL; gravity = Gravity.CENTER }
        val okBtn = Button(this).apply { text = "OK"; setTextColor(Color.WHITE); setOnClickListener { vibrate(); sendDisclaimerShown = true; getSharedPreferences("meshok", MODE_PRIVATE).edit().putBoolean("sendDisc", true).apply(); try { wm.removeView(popup) } catch (_: Exception) {}; showSendPopup() } }; styleButton(okBtn); btns.addView(okBtn)
        btns.addView(TextView(this).apply { layoutParams = LinearLayout.LayoutParams(24, 1) })
        val cancelBtn = Button(this).apply { text = "ОТМЕНА"; setTextColor(Color.WHITE); setOnClickListener { vibrate(); try { wm.removeView(popup) } catch (_: Exception) {} } }; styleButton(cancelBtn); btns.addView(cancelBtn)
        popup.addView(btns); wm.addView(popup, WindowManager.LayoutParams(WindowManager.LayoutParams.WRAP_CONTENT, WindowManager.LayoutParams.WRAP_CONTENT, if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY else WindowManager.LayoutParams.TYPE_PHONE, WindowManager.LayoutParams.FLAG_NOT_TOUCH_MODAL or WindowManager.LayoutParams.FLAG_WATCH_OUTSIDE_TOUCH, PixelFormat.TRANSLUCENT).apply { gravity = Gravity.CENTER })
    }

    private fun showNodeIdPopup() {
        val popup = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL; setPadding(24, 24, 24, 24) }; stylePopupBg(popup)
        popup.addView(TextView(this).apply { text = "Введите ID вашей ноды"; textSize = 14f; setTextColor(Color.WHITE); setPadding(0, 0, 0, 8) })
        val input = EditText(this).apply { setText(myNodeId); setTextColor(Color.WHITE); hint = "1234567890" }; styleInput(input); popup.addView(input)
        val btns = LinearLayout(this).apply { orientation = LinearLayout.HORIZONTAL; gravity = Gravity.CENTER; setPadding(0, 16, 0, 0) }
        val okBtn = Button(this).apply { text = "OK"; setTextColor(Color.WHITE); setOnClickListener { vibrate(); myNodeId = input.text.toString().trim(); getSharedPreferences("meshok", MODE_PRIVATE).edit().putString("myNodeId", myNodeId).apply(); try { wm.removeView(popup) } catch (_: Exception) {}; showSendPopup() } }; styleButton(okBtn); btns.addView(okBtn)
        btns.addView(TextView(this).apply { layoutParams = LinearLayout.LayoutParams(24, 1) })
        val cancelBtn = Button(this).apply { text = "ОТМЕНА"; setTextColor(Color.WHITE); setOnClickListener { vibrate(); try { wm.removeView(popup) } catch (_: Exception) {} } }; styleButton(cancelBtn); btns.addView(cancelBtn)
        popup.addView(btns); wm.addView(popup, WindowManager.LayoutParams(WindowManager.LayoutParams.WRAP_CONTENT, WindowManager.LayoutParams.WRAP_CONTENT, if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY else WindowManager.LayoutParams.TYPE_PHONE, WindowManager.LayoutParams.FLAG_NOT_TOUCH_MODAL or WindowManager.LayoutParams.FLAG_WATCH_OUTSIDE_TOUCH, PixelFormat.TRANSLUCENT).apply { gravity = Gravity.CENTER })
    }

    private fun showSettingsPopup() {
        vibrate(); if (settingsOverlay != null) return
        val scrollLayout = ScrollView(this).apply { layoutParams = LinearLayout.LayoutParams(600, 500) }
        val popup = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL; setPadding(24, 24, 24, 24) }; stylePopupBg(popup)
        popup.addView(TextView(this).apply { text = "⚙ Настройки"; textSize = 16f; setTextColor(Color.parseColor("#FFA500")); setPadding(0, 0, 0, 16) })
        val mqttCheck = Switch(this).apply { text = "MQTT"; setTextColor(Color.WHITE); isChecked = showMqtt }; popup.addView(mqttCheck)
        val onemeshCheck = Switch(this).apply { text = "ONEmesh"; setTextColor(Color.WHITE); isChecked = showOnemesh }; popup.addView(onemeshCheck)
        popup.addView(TextView(this).apply { text = "Мой ID ноды"; setTextColor(Color.WHITE) }); val nodeIdE = EditText(this).apply { setText(myNodeId); setTextColor(Color.WHITE) }; styleInput(nodeIdE); popup.addView(nodeIdE)
        popup.addView(TextView(this).apply { text = "Часовой пояс"; setTextColor(Color.WHITE) }); val tzE = EditText(this).apply { setText(timezoneOffset); setTextColor(Color.WHITE); hint = "+3" }; styleInput(tzE); popup.addView(tzE)
        popup.addView(TextView(this).apply { text = "ONEmesh URL"; setTextColor(Color.WHITE) }); val onemeshE = EditText(this).apply { setText(onemeshUrl); setTextColor(Color.WHITE) }; styleInput(onemeshE); popup.addView(onemeshE)
        popup.addView(TextView(this).apply { text = "Брокер"; setTextColor(Color.WHITE) }); val brokerE = EditText(this).apply { setText(brokerUrl); setTextColor(Color.WHITE) }; styleInput(brokerE); popup.addView(brokerE)
        popup.addView(TextView(this).apply { text = "Логин"; setTextColor(Color.WHITE) }); val userE = EditText(this).apply { setText(username); setTextColor(Color.WHITE) }; styleInput(userE); popup.addView(userE)
        popup.addView(TextView(this).apply { text = "Пароль"; setTextColor(Color.WHITE) }); val passE = EditText(this).apply { setText(password); setTextColor(Color.WHITE) }; styleInput(passE); popup.addView(passE)
        popup.addView(TextView(this).apply { text = "ID ноды для телем/позиций"; setTextColor(Color.WHITE) }); val filterE = EditText(this).apply { setText(filterNodeId); setTextColor(Color.WHITE); hint = "!69857734"; setHintTextColor(Color.parseColor("#AAAAAA")) }; styleInput(filterE); popup.addView(filterE)
        val telCheck = CheckBox(this).apply { text = "📡 Телеметрия"; setTextColor(Color.WHITE); isChecked = showTelemetry }; popup.addView(telCheck)
        val posCheck = CheckBox(this).apply { text = "🌍 Позиции"; setTextColor(Color.WHITE); isChecked = showPosition }; popup.addView(posCheck)
        val nodeCheck = CheckBox(this).apply { text = "🖥 Узлы"; setTextColor(Color.WHITE); isChecked = showNodes }; popup.addView(nodeCheck)
        popup.addView(TextView(this).apply { text = "Регион"; setTextColor(Color.WHITE) })
        val regSpin = Spinner(this).apply { val a = object : ArrayAdapter<String>(this@MeshtasticMqttOverlayService, android.R.layout.simple_spinner_item, regions.keys.toList()) { override fun getView(p: Int, c: View?, pa: ViewGroup): View { val v = super.getView(p, c, pa); (v as TextView).setTextColor(Color.parseColor("#AAAAAA")); return v }; override fun getDropDownView(p: Int, c: View?, pa: ViewGroup): View { val v = super.getDropDownView(p, c, pa); (v as TextView).setTextColor(Color.parseColor("#AAAAAA")); return v } }; a.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item); adapter = a; setSelection(regions.keys.indexOfFirst { regions[it] == currentRegion }.coerceAtLeast(0)) }; popup.addView(regSpin)
        popup.addView(TextView(this).apply { text = "Свой город (msh/RU/XXX)"; setTextColor(Color.WHITE) }); val customE = EditText(this).apply { setText(customRegion); setTextColor(Color.WHITE); hint = "msh/RU/MSK"; setHintTextColor(Color.parseColor("#AAAAAA")) }; styleInput(customE); popup.addView(customE)
        val btns = LinearLayout(this).apply { orientation = LinearLayout.HORIZONTAL; setPadding(0, 24, 0, 0); gravity = Gravity.CENTER }
        val okBtn = Button(this).apply { text = "OK"; setTextColor(Color.WHITE); setOnClickListener { vibrate()
            brokerUrl = brokerE.text.toString().trim(); username = userE.text.toString().trim(); password = passE.text.toString().trim(); filterNodeId = filterE.text.toString().trim()
            showTelemetry = telCheck.isChecked; showPosition = posCheck.isChecked; showNodes = nodeCheck.isChecked
            showMqtt = mqttCheck.isChecked; showOnemesh = onemeshCheck.isChecked; onemeshUrl = onemeshE.text.toString().trim()
            timezoneOffset = tzE.text.toString().trim().ifEmpty { "+3" }; myNodeId = nodeIdE.text.toString().trim()
            val selReg = regions[regSpin.selectedItem.toString()] ?: "RU/MSK"; val customVal = customE.text.toString().trim()
            if (customVal.isNotBlank()) { customRegion = customVal; currentRegion = "custom" } else if (selReg == "__custom__") { customRegion = "msh/RU/MSK"; currentRegion = "custom" } else { currentRegion = selReg; customRegion = "" }
            setTopics(); updateCounters()
            messagesContainer.removeAllViews(); seenHashes.clear(); msgViewMap.clear(); seenDedup.clear(); initialMessagesLoaded = false; if (showOnemesh) connectWebSocket()
            getSharedPreferences("meshok", MODE_PRIVATE).edit().putString("broker", brokerUrl).putString("user", username).putString("pass", password).putString("region", currentRegion).putString("custom_region", customRegion).putString("filter", filterNodeId).putBoolean("showTel", showTelemetry).putBoolean("showPos", showPosition).putBoolean("showNodes", showNodes).putBoolean("showMqtt", showMqtt).putBoolean("showOnemesh", showOnemesh).putString("onemeshUrl", onemeshUrl).putString("timezone", timezoneOffset).putString("myNodeId", myNodeId).apply()
            if (showMqtt) connectMqtt() else try { mqttClient?.disconnect(); mqttClient?.close() } catch (_: Exception) {}
            if (showOnemesh) connectWebSocket() else wsClient?.close(1000, "")
            dismissSettingsPopup()
        }}; styleButton(okBtn); btns.addView(okBtn)
        btns.addView(TextView(this).apply { layoutParams = LinearLayout.LayoutParams(24, 1) })
        val cancelBtn = Button(this).apply { text = "ОТМЕНА"; setTextColor(Color.WHITE); setOnClickListener { vibrate(); dismissSettingsPopup() } }; styleButton(cancelBtn); btns.addView(cancelBtn)
        popup.addView(btns); scrollLayout.addView(popup); settingsOverlay = scrollLayout
        wm.addView(scrollLayout, WindowManager.LayoutParams(WindowManager.LayoutParams.WRAP_CONTENT, WindowManager.LayoutParams.WRAP_CONTENT, if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY else WindowManager.LayoutParams.TYPE_PHONE, WindowManager.LayoutParams.FLAG_NOT_TOUCH_MODAL or WindowManager.LayoutParams.FLAG_WATCH_OUTSIDE_TOUCH, PixelFormat.TRANSLUCENT).apply { gravity = Gravity.CENTER; softInputMode = WindowManager.LayoutParams.SOFT_INPUT_ADJUST_RESIZE })
    }

    private fun dismissSettingsPopup() { settingsOverlay?.let { try { wm.removeView(it) } catch (_: Exception) {} }; settingsOverlay = null }

    private fun setupOverlayWindow() {
        wm = getSystemService(WINDOW_SERVICE) as WindowManager
        val main = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL; setPadding(0, 0, 0, 0) }; styleMainBg(main)
        val topicRow = LinearLayout(this).apply { orientation = LinearLayout.HORIZONTAL; gravity = Gravity.CENTER_VERTICAL; setBackgroundColor(Color.parseColor("#11221144")); setPadding(2, 0, 2, 0) }
        topicText = TextView(this).apply { text = "👤0 🌐0 🟢0"; textSize = 9f; setTextColor(Color.parseColor("#88FF88")); maxLines = 1; setSingleLine(true); layoutParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f) }
        topicRow.addView(topicText); topicRow.addView(View(this).apply { layoutParams = LinearLayout.LayoutParams(3, 1) })
        topicRow.addView(makeBtn("●", Color.parseColor("#FFA500")) { showLogPopup() }); topicRow.addView(View(this).apply { layoutParams = LinearLayout.LayoutParams(3, 1) })
        topicRow.addView(makeBtn("⚙", Color.parseColor("#FFA500")) { showSettingsPopup() }); topicRow.addView(View(this).apply { layoutParams = LinearLayout.LayoutParams(3, 1) })
        topicRow.addView(makeBtn("▶", Color.parseColor("#FFA500")) { showSendPopup() }); topicRow.addView(View(this).apply { layoutParams = LinearLayout.LayoutParams(3, 1) })
        topicRow.addView(makeBtn("–", Color.parseColor("#FFA500")) { collapseWindow() }); topicRow.addView(View(this).apply { layoutParams = LinearLayout.LayoutParams(3, 1) })
        topicRow.addView(makeBtn("✕", Color.parseColor("#FF5555")) { vibrate(); stopSelf() })
        main.addView(topicRow)
        statusText = TextView(this).apply { text = "Подключено"; textSize = 9f; setTextColor(Color.parseColor("#AAAAAA")); setBackgroundColor(Color.parseColor("#11221144")); setPadding(4, 1, 4, 1); maxLines = 1; setSingleLine(true) }
        main.addView(statusText); main.addView(View(this).apply { layoutParams = LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, 1); setBackgroundColor(Color.parseColor("#33FFFFFF")) })
        scrollDownButton = Button(this).apply { text = "▼"; textSize = 14f; setTextColor(Color.WHITE); visibility = View.GONE; setOnClickListener { scrollView.fullScroll(View.FOCUS_DOWN); isUserScrolling = false; it.visibility = View.GONE } }; styleButton(scrollDownButton)
        scrollView = ScrollView(this).apply { layoutParams = LinearLayout.LayoutParams(950, 800); setOnScrollChangeListener { _, _, _, _, _ -> val v = getChildAt(0) as? View; if (v != null) { isUserScrolling = v.bottom - (height + scrollY) > 60; scrollDownButton.visibility = if (isUserScrolling) View.VISIBLE else View.GONE } } }
        messagesContainer = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL }; scrollView.addView(messagesContainer)
        messagesContainer.setPadding(0, 0, 0, 40)
        main.addView(scrollView); main.addView(scrollDownButton); overlayView = main
        lp = WindowManager.LayoutParams(WindowManager.LayoutParams.WRAP_CONTENT, WindowManager.LayoutParams.WRAP_CONTENT, if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY else WindowManager.LayoutParams.TYPE_PHONE, WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE, PixelFormat.TRANSLUCENT).apply { gravity = Gravity.TOP or Gravity.START; x = 40; y = 120 }
        val drag = object : View.OnTouchListener { private var sx=0; private var sy=0; private var tsx=0f; private var tsy=0f; override fun onTouch(v: View, e: MotionEvent): Boolean { when (e.action) { MotionEvent.ACTION_DOWN -> { sx=lp!!.x; sy=lp!!.y; tsx=e.rawX; tsy=e.rawY; return true }
                    MotionEvent.ACTION_UP -> { snapToEdge(lp!!, overlayView, wm, resources.displayMetrics.widthPixels); return true }; MotionEvent.ACTION_MOVE -> { lp!!.x = sx + (e.rawX - tsx).toInt(); lp!!.y = sy + (e.rawY - tsy).toInt(); wm.updateViewLayout(overlayView, lp); return true } }; return false } }
        topicRow.setOnTouchListener(drag); statusText.setOnTouchListener(drag); wm.addView(overlayView, lp)
    }

    private fun makeBtn(text: String, color: Int, action: () -> Unit): Button = Button(this).apply { this.text = text; textSize = 13f; setBackgroundColor(Color.TRANSPARENT); setTextColor(color); setPadding(1, 4, 1, 4); setMinWidth(0); setMinHeight(0); val w = (50 * resources.displayMetrics.density).toInt(); layoutParams = LinearLayout.LayoutParams(w, LinearLayout.LayoutParams.WRAP_CONTENT); setOnClickListener { vibrate(); action() } }
    
    private fun collapseWindow() { 
        vibrate(); if (miniOverlay != null) return
        miniOverlay = TextView(this).apply { text = "📡"; textSize = 18f; setTextColor(Color.parseColor("#FFA500")); setBackgroundColor(Color.parseColor("#CC1A1A2E")); setPadding(18,18,18,18); gravity = Gravity.CENTER; background = GradientDrawable().apply { shape = GradientDrawable.RECTANGLE; cornerRadius = 32f; setColor(Color.parseColor("#CC1A1A2E")) } }
        val mlp = WindowManager.LayoutParams(WindowManager.LayoutParams.WRAP_CONTENT, WindowManager.LayoutParams.WRAP_CONTENT, if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY else WindowManager.LayoutParams.TYPE_PHONE, WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE, PixelFormat.TRANSLUCENT).apply { gravity = Gravity.TOP or Gravity.START; x = 40; y = 200 }
        miniOverlay!!.setOnTouchListener(object : View.OnTouchListener { private var sx=0; private var sy=0; private var tsx=0f; private var tsy=0f; override fun onTouch(v: View, e: MotionEvent): Boolean { when (e.action) { MotionEvent.ACTION_DOWN -> { sx=mlp.x; sy=mlp.y; tsx=e.rawX; tsy=e.rawY; return true }; MotionEvent.ACTION_MOVE -> { mlp.x = sx + (e.rawX - tsx).toInt(); mlp.y = sy + (e.rawY - tsy).toInt(); wm.updateViewLayout(miniOverlay, mlp); return true }; MotionEvent.ACTION_UP -> { if (Math.abs(e.rawX - tsx) < 10 && Math.abs(e.rawY - tsy) < 10) expandWindow() else snapToEdge(mlp, miniOverlay!!, wm, resources.displayMetrics.widthPixels) } }; return false } }); wm.addView(miniOverlay, mlp); try { wm.removeView(overlayView) } catch (_: Exception) {} 
    }
    
    private fun expandWindow() { 
        vibrate(); miniOverlay?.let { try { wm.removeView(it) } catch (_: Exception) {} }; miniOverlay = null
        wm.addView(overlayView, WindowManager.LayoutParams(WindowManager.LayoutParams.WRAP_CONTENT, WindowManager.LayoutParams.WRAP_CONTENT, if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) WindowManager.LayoutParams.TYPE_APPLICATION_OVERLAY else WindowManager.LayoutParams.TYPE_PHONE, WindowManager.LayoutParams.FLAG_NOT_FOCUSABLE, PixelFormat.TRANSLUCENT).apply { gravity = Gravity.TOP or Gravity.START; x = 40; y = 120 }) 
    }

    private fun connectMqtt() {
        if (!showMqtt) return; setStatus("Подключение...", Color.parseColor("#FFA500")); debugLog("MQTT: подключение к $brokerUrl...")
        try { mqttClient?.disconnect(); mqttClient?.close(); mqttClient = MqttClient(brokerUrl, MqttClient.generateClientId(), MemoryPersistence()); mqttClient?.connect(MqttConnectOptions().apply { userName=username; password=this@MeshtasticMqttOverlayService.password.toCharArray(); isCleanSession=true; connectionTimeout=10; keepAliveInterval=30; isAutomaticReconnect=true; maxInflight=100 }); reconnectAttempts = 0; setStatus("Подключено"); debugLog("MQTT: подключено"); for (t in topics) { mqttClient?.subscribe(t, 1); debugLog("MQTT: подписка на $t") } } catch (e: Exception) { reconnectAttempts++; val delay = 5000L * reconnectAttempts.coerceAtMost(12); setStatus("Ошибка (попытка $reconnectAttempts)", Color.parseColor("#FF5555")); debugLog("MQTT ошибка: ${e.message}"); handler.postDelayed({ connectMqtt() }, delay); return }
        mqttClient?.setCallback(object : MqttCallback {
            override fun connectionLost(c: Throwable?) { reconnectAttempts++; val delay = 5000L * reconnectAttempts.coerceAtMost(12); setStatus("Потеряно ($reconnectAttempts)", Color.parseColor("#FF5555")); debugLog("MQTT потеряно, попытка $reconnectAttempts"); handler.postDelayed({ connectMqtt() }, delay) }
            override fun messageArrived(topic: String?, msg: MqttMessage?) {
                if (!showMqtt || topic == null || msg == null) return
                val str = String(msg.payload, Charsets.UTF_8)
                try { val j = JSONObject(str); val type = j.optString("type","?"); val fromId = j.optLong("from", 0); var sender = if (fromId != 0L) String.format("!%08x", fromId).take(9) else j.optString("sender", "?"); if (sender == "?") sender = "MQTT"; var rssi = j.optInt("rssi", Int.MIN_VALUE); if (rssi == 0) rssi = Int.MIN_VALUE; val snr = j.optDouble("snr", Double.MIN_VALUE); val hops = j.optInt("hops_away", -1); val ts = j.optLong("timestamp", 0) * 1000L; val time = if (ts > 0) timeFormat.format(Date(ts)) else ""; val parts = topic.split("/"); val chFromTopic = if (parts.getOrNull(4) == "json") parts.getOrNull(5) ?: "CH?" else parts.getOrNull(4) ?: "CH?"; val ch = when(chFromTopic){"LongFast"->"LF";"MediumFast"->"MF";"MediumSlow"->"MS";"ShortSlow"->"SS";"LongSlow"->"LS";else->chFromTopic.take(4)}; val cityFromTopic = parts.getOrNull(2) ?: ""; val regionDisplay = if (cityFromTopic.isNotEmpty() && cityFromTopic != "json") " $cityFromTopic" else ""; val p = j.optJSONObject("payload"); val sn2 = p?.optString("shortname", j.optString("short_name", "")) ?: ""; val ln2 = p?.optString("longname", j.optString("long_name", "")) ?: ""; val now = System.currentTimeMillis(); if (sn2.isNotEmpty() || ln2.isNotEmpty()) { val old = nodeDb[sender]; val newShort = if (sn2.isNotEmpty()) sn2.take(4) else old?.first ?: ""; val newLong = if (ln2.isNotEmpty()) ln2 else old?.second ?: ""; nodeDb[sender] = Triple(newShort, newLong, now); saveNodeDb() } else if (!nodeDb.containsKey(sender)) { nodeDb[sender] = Triple("", "", now); saveNodeDb() }; val (dbShort, dbLong) = nodeDb[sender]?.let { Pair(it.first, it.second) } ?: Pair("", ""); val displayName = if (dbShort.isNotEmpty()) dbShort else if (dbLong.isNotEmpty()) dbLong.take(16) else sender; if (nodeDb.size % 10 == 0) updateCounters(); val matched = filterNodeId.isBlank() || sender.contains(filterNodeId.replace("!","")); val show = when (type) { "text" -> true; "telemetry" -> matched && showTelemetry; "position" -> matched && showPosition; "nodeinfo" -> matched && showNodes; else -> j.has("text") || j.has("barometric_pressure") || j.has("temperature") }; if (!show) return; val txt: String = when (type) { "text" -> { val t = j.optJSONObject("payload")?.optString("text", "") ?: ""; if (t.isBlank()) return; t }; "telemetry" -> { val t = fmtTel(j.optJSONObject("payload") ?: j); if (t == "?" || t.startsWith("{")) return; t }; "position" -> fmtPos(p); "nodeinfo" -> { if (!showNodes) return; "узел ${if (dbLong.isNotEmpty()) dbLong else sender}" }; else -> str.take(100) }; val url = if (type == "position") { val lat = p?.optLong("latitude_i", Long.MIN_VALUE) ?: Long.MIN_VALUE; val lng = p?.optLong("longitude_i", Long.MIN_VALUE) ?: Long.MIN_VALUE; if (lat != Long.MIN_VALUE && lng != Long.MIN_VALUE) "https://maps.google.com/?q=${lat/1e7},${lng/1e7}" else null } else null; handler.post { addOrUpdateMessage(CachedMessage(hash(sender, ch, txt), displayName, sender, txt, ch, regionDisplay, rssi, snr, hops, time, str, url, "MQTT", ts * 1000L)) } } catch (e: Exception) { debugLog("MQTT ошибка: ${e.message}") }
            }
            override fun deliveryComplete(t: IMqttDeliveryToken?) {}
        })
    }

    private fun fmtTel(p: Any?): String = if (p is JSONObject) buildString { p.optDouble("barometric_pressure",-1.0).let{if(it>0)append("🌡${"%.1f".format(it)}hPa ")}; p.optDouble("temperature",-100.0).let{if(it>-100)append("🌡${"%.1f".format(it)}°C ")}; p.optDouble("voltage",-1.0).let{if(it>0)append("⚡${"%.1f".format(it)}V ")}; p.optInt("battery_level",-1).let{if(it>=0)append("🔋${it}% ")}; p.optDouble("channel_utilization",-1.0).let{if(it>=0)append("📶${"%.1f".format(it)}% ")}; p.optDouble("air_util_tx",-1.0).let{if(it>=0)append("📤${"%.1f".format(it)}% ")}; p.optInt("uptime_seconds",-1).let{if(it>0)append("⏱${it/3600}ч")} }.trim().ifEmpty { p.toString() } else "?"
    private fun fmtPos(p: JSONObject?): String = if (p != null) { val lat = p.optLong("latitude_i", Long.MIN_VALUE); val lng = p.optLong("longitude_i", Long.MIN_VALUE); if (lat != Long.MIN_VALUE && lng != Long.MIN_VALUE) "${"%.4f".format(lat/1e7)},${"%.4f".format(lng/1e7)}${if(p.optInt("altitude",-1)>0)" ⬆${p.optInt("altitude")}м" else ""}${if(p.optInt("sats_in_view",0)>0)" 🛰${p.optInt("sats_in_view")}" else ""}" else p.toString() } else "?"
    private fun colorFromName(n: String): Int { var h=7; for(c in n)h=h*31+c.code; return Color.argb(170,(h shr 16)and 0xFF,(h shr 8)and 0xFF,h and 0xFF) }
    private fun startForegroundNotification() { try { val id = "meshok"; if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) getSystemService(NotificationManager::class.java).createNotificationChannel(NotificationChannel(id, "Overlay", NotificationManager.IMPORTANCE_LOW)); startForeground(1, NotificationCompat.Builder(this, id).setContentTitle("MeshOK").setContentText("Активен").setSmallIcon(android.R.drawable.ic_dialog_info).setOngoing(true).setPriority(NotificationCompat.PRIORITY_LOW).build()) } catch (e: Exception) { debugLog("Ошибка уведомления: ${e.message}") } }
    
    override fun onDestroy() { 
        super.onDestroy(); pingRunnable?.let { handler.removeCallbacks(it) }; wsReconnectRunnable?.let { handler.removeCallbacks(it) }; dismissSettingsPopup()
        try { logOverlay?.let { wm.removeView(it) } } catch (_: Exception) {}
        sendOverlay?.let { try { wm.removeView(it) } catch (_: Exception) {} }; miniOverlay?.let { try { wm.removeView(it) } catch (_: Exception) {} }
        wsClient?.close(1000, ""); try { mqttClient?.disconnect(); mqttClient?.close() } catch (_: Exception) {}
        try { wm.removeView(overlayView) } catch (_: Exception) {}
        debugLog("Сервис остановлен")
    }
}
