package worker

import (
	"runtime"
	"runtime/debug"
)

type buildInfo struct {
	Version   string
	Commit    string
	BuildTime string
	GoVersion string
}

func readBuildInfo() buildInfo {
	info := buildInfo{
		Version:   "dev",
		GoVersion: runtime.Version(),
	}

	build, ok := debug.ReadBuildInfo()
	if !ok {
		return info
	}

	if build.Main.Version != "" && build.Main.Version != "(devel)" {
		info.Version = build.Main.Version
	}

	for _, setting := range build.Settings {
		switch setting.Key {
		case "vcs.revision":
			info.Commit = setting.Value
		case "vcs.time":
			info.BuildTime = setting.Value
		default:
			continue
		}
	}

	return info
}
