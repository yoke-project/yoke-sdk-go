module github.com/yoke-project/yoke-sdk-go

go 1.26

require (
	github.com/yoke-project/yoke/proto v0.2.1-0.20261004185203-e90e48a3d22f
	go.yaml.in/yaml/v3 v3.0.5
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.12
)

require (
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
)

// v0.3.0 was tagged while its SDK line still said 0.2.1, so its release published nothing.
retract v0.3.0
