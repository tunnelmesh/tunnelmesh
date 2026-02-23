// Package web embeds static web assets for the coordinator admin panel.

package web

import "embed"

// Assets contains embedded static files (HTML, CSS, JS) for the web UI.

//go:embed index.html favicon.png logo.png css/* js/* js/lib/* img/*
var Assets embed.FS
