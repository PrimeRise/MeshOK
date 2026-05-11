package com.ospeo.meshok

import android.view.WindowManager

fun snapToEdge(lp: WindowManager.LayoutParams, view: android.view.View, wm: WindowManager, sw: Int) {
    lp.x = if (lp.x + view.width / 2 < sw / 2) 0 else sw - view.width
    wm.updateViewLayout(view, lp)
}
