package version

import "fmt"

const logo = `

   ___  _        ___    __ ______   ___   ____
  /  _]| |      /  _]  /  ]      | /   \ |    \
 /  [_ | |     /  [_  /  /|      ||     ||  D  )
|    _]| |___ |    _]/  / |_|  |_||  O  ||    /
|   [_ |     ||   [_/   \_  |  |  |     ||    \
|     ||     ||     \     | |  |  |     ||  .  \
|_____||_____||_____|\____| |__|   \___/ |__|\_|
`

const Mark = `+----------------------+------------------------------------------+`

// These variables are populated via the Go linker.
var (
	UTCBuildTime  = "unknown"
	ClientVersion = "unknown"
	GoVersion     = "unknown"
	GitBranch     = "unknown"
	GitTag        = "unknown"
	GitHash       = "unknown"
)

var Version = fmt.Sprintf("%s\n%s\n| % -20s | % -40s |\n| % -20s | % -40s |\n| % -20s | % -40s |\n| % -20s | % -40s |\n| % -20s | % -40s |\n| % -20s | % -40s |\n%s\n",
	logo,
	Mark,
	"Client Version", ClientVersion,
	"Go Version", GoVersion,
	"UTC Build Time", UTCBuildTime,
	"Git Branch", GitBranch,
	"Git Tag", GitTag,
	"Git Hash", GitHash,
	Mark)
