/*
 * Copyright (c) 2023 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package schedulerx

import (
	_ "embed"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Independent schema from upstream v1.0.13, not derived from current Go types.
//
//go:embed testdata/worker_v1_0_13.pb
var legacyWorkerSchema []byte

//go:embed testdata/worker_v1_0_10.pb
var legacyDeployedSchema []byte

func legacyDescriptors(t *testing.T, schema []byte) *protoregistry.Files {
	t.Helper()
	var set descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(schema, &set); err != nil {
		t.Fatal(err)
	}
	files, err := protodesc.NewFiles(&set)
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func TestExistingProtocolFieldsRemainWireCompatible(t *testing.T) {
	for version, schema := range map[string][]byte{"v1.0.10": legacyDeployedSchema, "v1.0.13": legacyWorkerSchema} {
		t.Run(version, func(t *testing.T) { checkExistingProtocol(t, legacyDescriptors(t, schema)) })
	}
}

func checkExistingProtocol(t *testing.T, files *protoregistry.Files) {
	var check func(protoreflect.MessageDescriptors)
	check = func(messages protoreflect.MessageDescriptors) {
		for i := 0; i < messages.Len(); i++ {
			old := messages.Get(i)
			found, err := protoregistry.GlobalFiles.FindDescriptorByName(old.FullName())
			if err != nil {
				t.Fatal(err)
			}
			current := found.(protoreflect.MessageDescriptor)
			for j := 0; j < old.Fields().Len(); j++ {
				field := old.Fields().Get(j)
				got := current.Fields().ByNumber(field.Number())
				if got == nil || got.Name() != field.Name() || got.Kind() != field.Kind() || got.Cardinality() != field.Cardinality() || got.IsPacked() != field.IsPacked() || got.HasPresence() != field.HasPresence() {
					t.Fatalf("old field %s changed its wire/presence contract", field.FullName())
				}
				if field.Message() != nil && got.Message().FullName() != field.Message().FullName() {
					t.Fatalf("message type changed: %s", field.FullName())
				}
				if field.Enum() != nil && got.Enum().FullName() != field.Enum().FullName() {
					t.Fatalf("enum type changed: %s", field.FullName())
				}
			}
			check(old.Messages())
		}
	}
	files.RangeFiles(func(file protoreflect.FileDescriptor) bool { check(file.Messages()); return true })
}

func TestNewEpochMessagesDecodeWithLegacySDK(t *testing.T) {
	for version, schema := range map[string][]byte{"v1.0.10": legacyDeployedSchema, "v1.0.13": legacyWorkerSchema} {
		t.Run(version, func(t *testing.T) { checkEpochMessages(t, legacyDescriptors(t, schema)) })
	}
}

func checkEpochMessages(t *testing.T, files *protoregistry.Files) {
	cases := []proto.Message{
		&MasterKillContainerRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(2), TaskId: proto.Int64(3), MayInterruptIfRunning: proto.Bool(false), SerialNum: proto.Int64(0), Attempt: proto.Int32(0)},
		&MasterDestroyContainerPoolRequest{JobInstanceId: proto.Int64(2), JobId: proto.Int64(1), SerialNum: proto.Int64(7), Attempt: proto.Int32(1)},
	}
	for _, current := range cases {
		t.Run(string(current.ProtoReflect().Descriptor().Name()), func(t *testing.T) {
			name := current.ProtoReflect().Descriptor().FullName()
			descriptor, err := files.FindDescriptorByName(name)
			if err != nil {
				t.Fatal(err)
			}
			old := dynamicpb.NewMessage(descriptor.(protoreflect.MessageDescriptor))
			data, err := proto.Marshal(current)
			if err != nil {
				t.Fatal(err)
			}
			if err := proto.Unmarshal(data, old); err != nil {
				t.Fatalf("old decoder rejected new sender: %v", err)
			}
			if len(old.GetUnknown()) == 0 {
				t.Fatal("test did not exercise new optional fields")
			}
			encoded, err := proto.Marshal(old)
			if err != nil {
				t.Fatal(err)
			}
			roundtrip := current.ProtoReflect().New().Interface()
			if err := proto.Unmarshal(encoded, roundtrip); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(current, roundtrip) {
				t.Fatal("legacy roundtrip lost known/new optional field values or explicit zero presence")
			}
			// An actual legacy publisher does not add unknown fields. Decode its
			// legacy-only payload through the new SDK and verify optional defaults.
			old.SetUnknown(nil)
			encoded, err = proto.Marshal(old)
			if err != nil {
				t.Fatal(err)
			}
			legacyInput := current.ProtoReflect().New().Interface()
			if err := proto.Unmarshal(encoded, legacyInput); err != nil {
				t.Fatal(err)
			}
			switch got := legacyInput.(type) {
			case *MasterKillContainerRequest:
				if got.Attempt != nil || got.SerialNum != nil || got.GetTaskId() != 3 {
					t.Fatalf("legacy Kill defaults changed: %v", got)
				}
			case *MasterDestroyContainerPoolRequest:
				if got.Attempt != nil || got.GetSerialNum() != 7 {
					t.Fatalf("legacy Destroy defaults changed: %v", got)
				}
			}
		})
	}
}

func TestRejectedTaskStatusIsReadableByLegacyMaster(t *testing.T) {
	for version, schema := range map[string][]byte{"v1.0.10": legacyDeployedSchema, "v1.0.13": legacyWorkerSchema} {
		t.Run(version, func(t *testing.T) { checkRejectedStatus(t, legacyDescriptors(t, schema)) })
	}
}

func checkRejectedStatus(t *testing.T, files *protoregistry.Files) {
	req := &ContainerReportTaskStatusRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(2), TaskId: proto.Int64(3), Status: proto.Int32(5), WorkerAddr: proto.String("127.0.0.1:12345"), WorkerId: proto.String("worker"), SerialNum: proto.Int64(7), Result: proto.String("container instance admission history is full")}
	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := files.FindDescriptorByName(req.ProtoReflect().Descriptor().FullName())
	if err != nil {
		t.Fatal(err)
	}
	old := dynamicpb.NewMessage(descriptor.(protoreflect.MessageDescriptor))
	if err := proto.Unmarshal(data, old); err != nil {
		t.Fatal(err)
	}
	fields := old.Descriptor().Fields()
	if old.Get(fields.ByName("status")).Int() != 5 || old.Get(fields.ByName("serial_num")).Int() != 7 || old.Get(fields.ByName("result")).String() != "container instance admission history is full" {
		t.Fatalf("legacy master lost rejection metadata: %v", old)
	}
}

// Attempt is optional so legacy publishers remain readable; explicit zero must
// survive old decoders as an unknown field rather than becoming absent metadata.
func TestStatusAttemptWireCompatibility(t *testing.T) {
	for version, schema := range map[string][]byte{"v1.0.10": legacyDeployedSchema, "v1.0.13": legacyWorkerSchema} {
		t.Run(version, func(t *testing.T) {
			files := legacyDescriptors(t, schema)
			for _, attempt := range []int32{0, 3} {
				cases := []proto.Message{
					&ContainerReportTaskStatusRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(2), TaskId: proto.Int64(3), Status: proto.Int32(4), WorkerAddr: proto.String("worker"), WorkerId: proto.String("id"), Attempt: proto.Int32(attempt)},
					&ContainerBatchReportTaskStatuesRequest{JobId: proto.Int64(1), JobInstanceId: proto.Int64(2), TaskMasterAkkaPath: proto.String("master"), WorkerAddr: proto.String("worker"), WorkerId: proto.String("id"), Attempt: proto.Int32(attempt), TaskStatues: []*TaskStatusInfo{{TaskId: proto.Int64(3), Status: proto.Int32(4)}}},
				}
				for _, message := range cases {
					name := message.ProtoReflect().Descriptor().FullName()
					descriptor, err := files.FindDescriptorByName(name)
					if err != nil {
						t.Fatal(err)
					}
					old := dynamicpb.NewMessage(descriptor.(protoreflect.MessageDescriptor))
					encoded, err := proto.Marshal(message)
					if err != nil {
						t.Fatal(err)
					}
					if err := proto.Unmarshal(encoded, old); err != nil {
						t.Fatal(err)
					}
					if len(old.GetUnknown()) == 0 {
						t.Fatal("new attempt field did not reach legacy decoder")
					}
					roundtrip, err := proto.Marshal(old)
					if err != nil {
						t.Fatal(err)
					}
					decoded := message.ProtoReflect().New().Interface()
					if err := proto.Unmarshal(roundtrip, decoded); err != nil {
						t.Fatal(err)
					}
					if !proto.Equal(message, decoded) {
						t.Fatal("legacy roundtrip lost explicit attempt", name, attempt)
					}
					old.SetUnknown(nil)
					legacyOnly, err := proto.Marshal(old)
					if err != nil {
						t.Fatal(err)
					}
					if err := proto.Unmarshal(legacyOnly, decoded); err != nil {
						t.Fatal(err)
					}
					field := decoded.ProtoReflect().Descriptor().Fields().ByName("attempt")
					if decoded.ProtoReflect().Has(field) {
						t.Fatal("legacy publisher acquired an explicit attempt")
					}
				}
			}
		})
	}
}
