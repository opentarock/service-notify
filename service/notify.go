package service

import (
	"log"

	"github.com/opentarock/service-api/go/proto"
	"github.com/opentarock/service-api/go/proto_errors"
	"github.com/opentarock/service-api/go/proto_notify"
	"github.com/opentarock/service-api/go/service"
)

type notifyServiceHandlers struct {
}

func NewNotifyServiceHandlers() *notifyServiceHandlers {
	return &notifyServiceHandlers{}
}

func (s *notifyServiceHandlers) MessageUsersHandler() service.MessageHandler {
	return service.MessageHandlerFunc(func(msg *proto.Message) proto.CompositeMessage {
		var notifyHeader proto_notify.MessageUsersHeader
		found, err := msg.Header.Unmarshal(&notifyHeader)
		if err != nil {
			log.Println(err)
			return proto.CompositeMessage{
				Message: proto_errors.NewMalformedMessage(notifyHeader.GetMessageType()),
			}
		} else if !found {
			log.Println("Missing header: proto_notify.MessageUsersHeader")
			return proto.CompositeMessage{
				Message: proto_errors.NewMissingHeader(notifyHeader.GetMessageType()),
			}
		}
		log.Printf("Sending message to users: %s", notifyHeader.GetUserIds())
		response := proto_notify.MessageUsersResponse{}
		return proto.CompositeMessage{Message: &response}
	})
}
