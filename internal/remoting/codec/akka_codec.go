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

package codec

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/alibaba/schedulerx-worker-go/internal/proto/akka"
	"github.com/alibaba/schedulerx-worker-go/internal/proto/schedulerx"
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
)

type Options struct {
	isMessageContainerSerializer bool
	selectionEnvelopePattern     []*akka.Selection
}

type Option func(*Options)

func WithMessageContainerSerializer() Option {
	return func(o *Options) {
		o.isMessageContainerSerializer = true
	}
}

func WithSelectionEnvelopePattern(pattern []*akka.Selection) Option {
	return func(o *Options) {
		o.selectionEnvelopePattern = pattern
	}
}

func EncodeAkkaMessage(rawMsg proto.Message, recipientPath, senderPath, manifest string, opts ...Option) (*akka.AkkaProtocolMessage, error) {
	var (
		payloadData []byte
		err         error
	)

	options := new(Options)
	for _, opt := range opts {
		opt(options)
	}

	marshaledMsg, err := proto.Marshal(rawMsg)
	if err != nil {
		return nil, err
	}

	if options.isMessageContainerSerializer {
		selectionEnvelope := &akka.SelectionEnvelope{
			EnclosedMessage: marshaledMsg,
			// use ProtobufSerializer
			// ref: https://github.com/akka/akka/blob/main/akka-remote/src/main/resources/reference.conf#L110
			SerializerId:    proto.Int32(2),
			Pattern:         options.selectionEnvelopePattern,
			MessageManifest: []byte(manifest),
			WildcardFanOut:  proto.Bool(false),
		}
		selectionEnvelopeData, err := proto.Marshal(selectionEnvelope)
		if err != nil {
			return nil, fmt.Errorf("Marshal SelectionEnvelope failed, err=%s ", err.Error())
		}
		serializedMessage := &akka.SerializedMessage{
			Message: selectionEnvelopeData,
			// use MessageContainerSerializer
			// ref: https://github.com/akka/akka/blob/main/akka-remote/src/main/resources/reference.conf#L112
			SerializerId: proto.Int32(6),
		}
		envelopeContainer := &akka.AckAndEnvelopeContainer{
			Envelope: &akka.RemoteEnvelope{
				Recipient: &akka.ActorRefData{
					Path: proto.String(recipientPath),
				},
				Message: serializedMessage,
				Sender: &akka.ActorRefData{
					Path: proto.String(senderPath),
				},
			},
		}
		payloadData, err = proto.Marshal(envelopeContainer)
		if err != nil {
			return nil, fmt.Errorf("Marshal AckAndEnvelopeContainer failed, err=%s ", err.Error())
		}
	} else {
		envelopeContainer := &akka.AckAndEnvelopeContainer{
			Envelope: &akka.RemoteEnvelope{
				Recipient: &akka.ActorRefData{
					Path: proto.String(recipientPath),
				},
				Message: &akka.SerializedMessage{
					Message: marshaledMsg,
					// use ProtobufSerializer
					// ref: https://github.com/akka/akka/blob/main/akka-remote/src/main/resources/reference.conf#L110
					SerializerId:    proto.Int32(2),
					MessageManifest: []byte(manifest),
				},
				Sender: &akka.ActorRefData{
					Path: proto.String(senderPath),
				},
			},
		}
		payloadData, err = proto.Marshal(envelopeContainer)
		if err != nil {
			return nil, fmt.Errorf("Marshal AckAndEnvelopeContainer failed, err=%s ", err.Error())
		}
	}

	return &akka.AkkaProtocolMessage{
		Payload: payloadData,
	}, nil
}

func DecodeAkkaMessage(msg *akka.AkkaProtocolMessage) (interface{}, string, error) {
	envelopeContainer := new(akka.AckAndEnvelopeContainer)
	if err := proto.Unmarshal(msg.Payload, envelopeContainer); err != nil {
		return nil, "", err
	}
	envelope := envelopeContainer.Envelope

	if msg.Instruction != nil {
		switch msg.Instruction.GetCommandType() {
		case akka.CommandType_ASSOCIATE,
			akka.CommandType_DISASSOCIATE,
			akka.CommandType_HEARTBEAT,
			akka.CommandType_DISASSOCIATE_SHUTTING_DOWN,
			akka.CommandType_DISASSOCIATE_QUARANTINED:
			return msg.GetInstruction(), "", nil
		default:
			return nil, "", fmt.Errorf("Unknown msg instruction=%s, decode failed ", msg.Instruction.GetCommandType())
		}
	} else if envelope != nil && envelope.Sender != nil && envelope.Message != nil {
		var (
			msgType    string
			err        error
			msgRawData []byte
		)
		switch *envelope.Message.SerializerId {
		case 2:
			// use ProtobufSerializer
			// ref: https://github.com/akka/akka/blob/main/akka-remote/src/main/resources/reference.conf#L110
			msgType, err = utils.GetMsgType(string(envelope.Message.MessageManifest))
			if err != nil {
				return fmt.Errorf("Get message type from manifest failed, err=%s ", err.Error()), "", nil
			}
			msgRawData = envelope.Message.Message
		case 6:
			// use MessageContainerSerializer
			// ref: https://github.com/akka/akka/blob/main/akka-remote/src/main/resources/reference.conf#L112
			innerMsg := new(akka.SelectionEnvelope)
			if err := proto.Unmarshal(envelope.Message.Message, innerMsg); err != nil {
				return fmt.Errorf("Unmarshal envelope.Message.Message to SelectionEnvelope failed, err=%s ", err.Error()), "", nil
			}
			msgType, err = utils.GetMsgType(string(innerMsg.MessageManifest))
			if err != nil {
				return fmt.Errorf("Get message type from manifest failed, err=%s ", err.Error()), "", nil
			}
			msgRawData = innerMsg.EnclosedMessage
		default:
			return fmt.Errorf("Unknown serializerId=%d in envelope.Message.Message ", *envelope.Message.SerializerId), "", nil
		}

		senderPath := envelope.Sender.GetPath()
		switch msgType {
		case "WorkerHeartBeatResponse":
			msg := new(schedulerx.WorkerHeartBeatResponse)
			if err := proto.Unmarshal(msgRawData, msg); err != nil {
				return nil, "", err
			}
			return msg, senderPath, nil
		case "ServerSubmitJobInstanceRequest":
			msg := new(schedulerx.ServerSubmitJobInstanceRequest)
			if err := proto.Unmarshal(msgRawData, msg); err != nil {
				return nil, "", err
			}
			return msg, senderPath, nil
		case "ServerKillJobInstanceRequest":
			msg := new(schedulerx.ServerKillJobInstanceRequest)
			if err := proto.Unmarshal(msgRawData, msg); err != nil {
				return nil, "", err
			}
			return msg, senderPath, nil
		case "ServerKillTaskRequest":
			msg := new(schedulerx.ServerKillTaskRequest)
			if err := proto.Unmarshal(msgRawData, msg); err != nil {
				return nil, "", err
			}
			return msg, senderPath, nil
		case "ServerCheckTaskMasterRequest":
			msg := new(schedulerx.ServerCheckTaskMasterRequest)
			if err := proto.Unmarshal(msgRawData, msg); err != nil {
				return nil, "", err
			}
			return msg, senderPath, nil
		case "MasterNotifyWorkerPullRequest":
			msg := new(schedulerx.MasterNotifyWorkerPullRequest)
			if err := proto.Unmarshal(msgRawData, msg); err != nil {
				return nil, "", err
			}
			return msg, senderPath, nil
		case "ServerThreadDumpRequest":
			msg := new(schedulerx.ServerThreadDumpRequest)
			if err := proto.Unmarshal(msgRawData, msg); err != nil {
				return nil, "", err
			}
			return msg, senderPath, nil
		case "ServerCallbackCalendarRequest":
			msg := new(schedulerx.ServerCallbackCalendarRequest)
			if err := proto.Unmarshal(msgRawData, msg); err != nil {
				return nil, "", err
			}
			return msg, senderPath, nil
		case "WorkerReportJobInstanceStatusResponse":
			msg := new(schedulerx.WorkerReportJobInstanceStatusResponse)
			if err := proto.Unmarshal(msgRawData, msg); err != nil {
				return nil, "", err
			}
			return msg, senderPath, nil
		default:
			return nil, "", fmt.Errorf("Unknown message type=%s, decode failed ", msgType)
		}
	}

	return nil, "", fmt.Errorf("Unknown message=%+v, decode failed ", msg)
}
