package com.ospeo.meshok

import android.graphics.Color
import android.graphics.drawable.GradientDrawable
import android.widget.Button
import android.widget.LinearLayout
import android.widget.TextView

fun styleButton(btn: Button) {
    btn.background = GradientDrawable().apply {
        shape = GradientDrawable.RECTANGLE
        cornerRadius = 18f
        setColor(Color.parseColor("#CC151530"))
        setStroke(1, Color.parseColor("#44FF6600"))
    }
    btn.setPadding(6, 2, 6, 2)
    btn.textSize = 11f
}

fun stylePopupBg(popup: LinearLayout) {
    popup.background = GradientDrawable().apply {
        shape = GradientDrawable.RECTANGLE
        cornerRadius = 24f
        setColor(Color.parseColor("#EE0B0B18"))
        setStroke(2, Color.parseColor("#44FF6600"))
    }
}

fun styleInput(input: TextView) {
    input.background = GradientDrawable().apply {
        shape = GradientDrawable.RECTANGLE
        cornerRadius = 10f
        setColor(Color.parseColor("#2A2A4E"))
        setStroke(1, Color.parseColor("#33FF6600"))
    }
    input.textSize = 12f
    input.setPadding(8, 6, 8, 6)
}

fun styleMainBg(view: android.view.View) {
    view.background = GradientDrawable().apply {
        shape = GradientDrawable.RECTANGLE
        cornerRadius = 28f
        setColor(Color.parseColor("#EE0B0B18"))
        setStroke(2, Color.parseColor("#44FF6600"))
    }
}
