package service

import (
	"strings"
	"sync"
	"time"

	"code.google.com/p/go.net/context"
	log "gopkg.in/inconshreveable/log15.v2"

	"github.com/opentarock/service-api/go/client"
	"github.com/opentarock/service-api/go/proto"
	"github.com/opentarock/service-api/go/proto_errors"
	"github.com/opentarock/service-api/go/proto_notify"
	"github.com/opentarock/service-api/go/proto_presence"
	"github.com/opentarock/service-api/go/reqcontext"
	"github.com/opentarock/service-api/go/service"
	"github.com/opentarock/service-api/go/util/contextutil"
)

const requestTimeout = 1 * time.Minute

type notifyServiceHandlers struct {
	presenceClient client.PresenceClient
	gcmClient      client.GcmClient
}

func NewNotifyServiceHandlers(presenceClient client.PresenceClient, gcmClient client.GcmClient) *notifyServiceHandlers {
	return &notifyServiceHandlers{
		presenceClient: presenceClient,
		gcmClient:      gcmClient,
	}
}

func (s *notifyServiceHandlers) MessageUsersHandler() service.MessageHandler {
	return service.MessageHandlerFunc(func(msg *proto.Message) proto.CompositeMessage {
		ctx, cancel := context.WithTimeout(reqcontext.NewContext(context.Background(), msg), requestTimeout)
		defer cancel()

		logger := reqcontext.ContextLogger(ctx)

		var notifyHeader proto_notify.MessageUsersHeader
		found, err := msg.Header.Unmarshal(&notifyHeader)
		errMsg := validateRequest(logger, &notifyHeader, found, err)
		if errMsg != nil {
			return *errMsg
		}

		var dataMessage proto_notify.TextMessage
		err = msg.Unmarshal(&dataMessage)
		if err != nil {
			logger.Warn("Error unmarshalling message", "msg_type", dataMessage.GetMessageType(), "error", err.Error())
			return proto.CompositeMessage{
				Message: proto_errors.NewMalformedMessage(dataMessage.GetMessageType()),
			}
		}

		devices, err := s.getAllDevices(ctx, notifyHeader.GetUserIds())

		logger.Info("Sending message to user's devices", "user_ids", strings.Join(notifyHeader.GetUserIds(), ","))
		s.sendMessage(ctx, dataMessage.GetData(), devices)

		response := proto_notify.MessageUsersResponse{}
		return proto.CompositeMessage{Message: &response}
	})
}

type deviceTypes map[proto_presence.Device_Type][]*proto_presence.Device

func (s *notifyServiceHandlers) getAllDevices(ctx context.Context, userIds []string) (deviceTypes, error) {
	devices := make(deviceTypes)
	results := make(chan *proto_presence.Device)
	var wg sync.WaitGroup
	// Start a new task for each user to get devices in parallel.
	for _, userId := range userIds {
		wg.Add(1)
		go func(userId string) {
			defer wg.Done()
			s.getAllUserDevices(ctx, userId, results)
		}(userId)
	}
	// Once all the started tasks are done close the result channel so we
	// know when all the tasks are done sending values.
	go func() {
		wg.Wait()
		close(results)
	}()
	cancel := make(chan struct{})
	err := contextutil.DoWithCancel(ctx, func() {
		cancel <- struct{}{}
	}, func() error {
		for {
			select {
			case device, received := <-results:
				if received {
					if _, ok := devices[device.GetType()]; !ok {
						devices[device.GetType()] = make([]*proto_presence.Device, 0)
					}
					devices[device.GetType()] = append(devices[device.GetType()], device)
				} else {
					// Channel was closed, either all tasks are done or the context
					// was cancelled.
					return nil
				}
			case <-cancel:
				break
			}
		}
	})

	if err != nil {
		return nil, err
	}
	return devices, nil
}

func (s *notifyServiceHandlers) getAllUserDevices(ctx context.Context, userId string, results chan<- *proto_presence.Device) {
	devicesResponse, err := s.presenceClient.GetUserDevices(ctx, userId)
	logger := reqcontext.ContextLogger(ctx)
	if err != nil {
		logger.Warn("Error getting devices for user", "error", err, "user_id", userId)
		return
	}
	for _, device := range devicesResponse.GetDevices() {
		results <- device
	}
}

func (s *notifyServiceHandlers) sendMessage(ctx context.Context, msg string, devices deviceTypes) {
	logger := reqcontext.ContextLogger(ctx)
	for deviceType, devicesList := range devices {
		switch deviceType {
		case proto_presence.Device_ANDROID_GCM:
			s.sendMessageAndroidGcm(ctx, msg, devicesList)
		default:
			logger.Warn("Unsupported device type", "device_type", deviceType.String())
		}
	}
}

func (s *notifyServiceHandlers) sendMessageAndroidGcm(ctx context.Context, msg string, devices []*proto_presence.Device) {
	logger := reqcontext.ContextLogger(ctx)
	regIds := make([]string, 0)
	for _, device := range devices {
		regIds = append(regIds, device.GetGcmRegistrationId())
	}
	sendResponse, err := s.gcmClient.SendMessage(ctx, regIds, msg, nil)
	if err != nil {
		logger.Warn("Error sending message over gcm", "error", err)
	} else if sendResponse.ErrorCode != nil {
		logger.Error("Unable to send message over gcm", "error_code", sendResponse.GetErrorCode().String())
	}
}

func validateRequest(logger log.Logger, h *proto_notify.MessageUsersHeader, found bool, err error) *proto.CompositeMessage {
	if err != nil {
		logger.Warn("Error unmarshalling header", "error", err.Error())
		return &proto.CompositeMessage{
			Message: proto_errors.NewMalformedMessage(h.GetMessageType()),
		}
	} else if !found {
		logger.Warn("Missing header", "error", "Header: proto_notify.MessageUsersHeader")
		return &proto.CompositeMessage{
			Message: proto_errors.NewMissingHeader(h.GetMessageType()),
		}
	}
	return nil
}
