package web

import "embed"

//go:embed *.html assets/*
var files embed.FS

var (
	SetupHTML     = mustRead("setup.html")
	LoginHTML     = mustRead("login.html")
	DashboardHTML = mustRead("dashboard.html")
	LogsHTML      = mustRead("logs.html")
	MediaHTML     = mustRead("media.html")
	ProfilesHTML  = mustRead("profiles.html")
	AppCSS        = mustRead("assets/app.css")
	AppJS         = mustRead("assets/app.js")
)

func mustRead(name string) []byte {
	data, err := files.ReadFile(name)
	if err != nil {
		panic(err)
	}
	return data
}
