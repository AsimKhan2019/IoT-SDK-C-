// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Microsoft.Azure.Devices.Client.Common;
using Microsoft.Azure.Devices.Client.Exceptions;
using Microsoft.Azure.Devices.Common;
using Microsoft.Azure.Devices.Shared;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Disconnecting;
using MQTTnet.Client.Options;
using MQTTnet.Client.Publishing;
using MQTTnet.Client.Subscribing;
using MQTTnet.Client.Unsubscribing;
using MQTTnet.Protocol;
using Newtonsoft.Json;

#if NET5_0

using TaskCompletionSource = System.Threading.Tasks.TaskCompletionSource;

#else
using TaskCompletionSource = Microsoft.Azure.Devices.Shared.TaskCompletionSource;
#endif

namespace Microsoft.Azure.Devices.Client.Transport.Mqtt
{
    internal sealed class MqttTransportHandler : TransportHandler
    {
        private const int ProtocolGatewayPort = 8883;
        private const int MaxMessageSize = 256 * 1024;
        private const int MaxTopicNameLength = 65535;

        private const string DeviceToCloudMessagesTopicFormat = "devices/{0}/messages/events/";
        private const string ModuleToCloudMessagesTopicFormat = "devices/{0}/modules/{1}/messages/events/";
        private string deviceToCloudMessagesTopic;


        // Topic names for receiving cloud-to-device messages.

        //TODO need # sign for all receiving topics!!!!!
        private const string DeviceBoundMessagesSubscriptionTopicFormat = "devices/{0}/messages/devicebound/#";
        private const string DeviceBoundMessagesReceivingTopicFormat = "devices/{0}/messages/devicebound/";
        private string deviceBoundMessagesSubscriptionTopic;
        private string deviceBoundMessagesReceivingTopic;


        // Topic names for retrieving a device's twin properties.
        // The client first subscribes to "$iothub/twin/res/#", to receive the operation's responses.
        // It then sends an empty message to the topic "$iothub/twin/GET/?$rid={request id}, with a populated value for request Id.
        // The service then sends a response message containing the device twin data on topic "$iothub/twin/res/{status}/?$rid={request id}", using the same request Id as the request.

        private const string TwinResponseTopic = "$iothub/twin/res/";
        private const string TwinGetTopic = "$iothub/twin/GET/?$rid={0}";
        private const string TwinResponseTopicPattern = @"\$iothub/twin/res/(\d+)/(\?.+)";
        private readonly Regex _twinResponseTopicRegex = new Regex(TwinResponseTopicPattern, RegexOptions.Compiled);

        // Topic name for updating device twin's reported properties.
        // The client first subscribes to "$iothub/twin/res/#", to receive the operation's responses.
        // The client then sends a message containing the twin update to "$iothub/twin/PATCH/properties/reported/?$rid={request id}", with a populated value for request Id.
        // The service then sends a response message containing the new ETag value for the reported properties collection on the topic "$iothub/twin/res/{status}/?$rid={request id}", using the same request Id as the request.
        private const string TwinReportedPropertiesPatchTopic = "$iothub/twin/PATCH/properties/reported/?$rid={0}";

        // Topic names for receiving twin desired property update notifications.

        private const string TwinDesiredPropertiesPatchTopic = "$iothub/twin/PATCH/properties/desired/";

        // Topic name for responding to direct methods.
        // The client first subscribes to "$iothub/methods/POST/#".
        // The service sends method requests to the topic "$iothub/methods/POST/{method name}/?$rid={request id}".
        // The client responds to the direct method invocation by sending a message to the topic "$iothub/methods/res/{status}/?$rid={request id}", using the same request Id as the request.

        private const string DirectMethodsReceivingTopicFormat = "$iothub/methods/POST/";
        private const string DirectMethodsSubscriptionTopicFormat = "$iothub/methods/POST/#";
        private const string MethodResponseTopic = "$iothub/methods/res/{0}/?$rid={1}";

        // Topic names for enabling events on Modules.

        private const string ReceiveEventMessageTopic = "devices/{0}/modules/{1}/";

        private const string DeviceClientTypeParam = "DeviceClientType";

        private const string SegmentSeparator = "/";

        private IMqttClient mqttClient;
        private IMqttClientOptions mqttClientOptions;

        private readonly Func<MethodRequestInternal, Task> _methodListener;
        private readonly Action<TwinCollection> _onDesiredStatePatchListener;
        private readonly Func<string, Message, Task> _moduleMessageReceivedListener;
        private readonly Func<Message, Task> _deviceMessageReceivedListener;

        private readonly ConcurrentQueue<Message> receivedCloudToDeviceMessages = new ConcurrentQueue<Message>();
        private readonly Dictionary<string, MqttApplicationMessageReceivedEventArgs> messagesToAcknowledge = new Dictionary<string, MqttApplicationMessageReceivedEventArgs>();

        private readonly Dictionary<string, Twin> receivedTwins = new Dictionary<string, Twin>();
        private SemaphoreSlim _getTwinSemaphore = new SemaphoreSlim(0);

        private readonly Dictionary<string, int> reportedPropertyUpdateResponses = new Dictionary<string, int>();
        private SemaphoreSlim _reportedPropertyUpdateResponsesSemaphore = new SemaphoreSlim(0);

        private SemaphoreSlim _receivingSemaphore = new SemaphoreSlim(0);

        private bool isSubscribedToCloudToDeviceMessages;

        private readonly string deviceId;
        private readonly string moduleId;

        // Used to correlate back to a received message when the user wants to acknowledge it. This is not a value
        // that is sent over the wire, so we increment this value locally instead.
        private int nextLockToken; 

        private static class IotHubWirePropertyNames
        {
            public const string AbsoluteExpiryTime = "$.exp";
            public const string CorrelationId = "$.cid";
            public const string MessageId = "$.mid";
            public const string To = "$.to";
            public const string UserId = "$.uid";
            public const string OutputName = "$.on";
            public const string MessageSchema = "$.schema";
            public const string CreationTimeUtc = "$.ctime";
            public const string ContentType = "$.ct";
            public const string ContentEncoding = "$.ce";
            public const string ConnectionDeviceId = "$.cdid";
            public const string ConnectionModuleId = "$.cmid";
            public const string MqttDiagIdKey = "$.diagid";
            public const string MqttDiagCorrelationContextKey = "$.diagctx";
            public const string InterfaceId = "$.ifid";
            public const string ComponentName = "$.sub";
        }

        private static readonly Dictionary<string, string> s_toSystemPropertiesMap = new Dictionary<string, string>
        {
            {IotHubWirePropertyNames.AbsoluteExpiryTime, MessageSystemPropertyNames.ExpiryTimeUtc},
            {IotHubWirePropertyNames.CorrelationId, MessageSystemPropertyNames.CorrelationId},
            {IotHubWirePropertyNames.MessageId, MessageSystemPropertyNames.MessageId},
            {IotHubWirePropertyNames.To, MessageSystemPropertyNames.To},
            {IotHubWirePropertyNames.UserId, MessageSystemPropertyNames.UserId},
            {IotHubWirePropertyNames.MessageSchema, MessageSystemPropertyNames.MessageSchema},
            {IotHubWirePropertyNames.CreationTimeUtc, MessageSystemPropertyNames.CreationTimeUtc},
            {IotHubWirePropertyNames.ContentType, MessageSystemPropertyNames.ContentType},
            {IotHubWirePropertyNames.ContentEncoding, MessageSystemPropertyNames.ContentEncoding},
            {MessageSystemPropertyNames.Operation, MessageSystemPropertyNames.Operation},
            {MessageSystemPropertyNames.Ack, MessageSystemPropertyNames.Ack},
            {IotHubWirePropertyNames.ConnectionDeviceId, MessageSystemPropertyNames.ConnectionDeviceId },
            {IotHubWirePropertyNames.ConnectionModuleId, MessageSystemPropertyNames.ConnectionModuleId },
            {IotHubWirePropertyNames.MqttDiagIdKey, MessageSystemPropertyNames.DiagId},
            {IotHubWirePropertyNames.MqttDiagCorrelationContextKey, MessageSystemPropertyNames.DiagCorrelationContext},
            {IotHubWirePropertyNames.InterfaceId, MessageSystemPropertyNames.InterfaceId}
        };

        private static readonly Dictionary<string, string> s_fromSystemPropertiesMap = new Dictionary<string, string>
        {
            {MessageSystemPropertyNames.ExpiryTimeUtc, IotHubWirePropertyNames.AbsoluteExpiryTime},
            {MessageSystemPropertyNames.CorrelationId, IotHubWirePropertyNames.CorrelationId},
            {MessageSystemPropertyNames.MessageId, IotHubWirePropertyNames.MessageId},
            {MessageSystemPropertyNames.To, IotHubWirePropertyNames.To},
            {MessageSystemPropertyNames.UserId, IotHubWirePropertyNames.UserId},
            {MessageSystemPropertyNames.MessageSchema, IotHubWirePropertyNames.MessageSchema},
            {MessageSystemPropertyNames.CreationTimeUtc, IotHubWirePropertyNames.CreationTimeUtc},
            {MessageSystemPropertyNames.ContentType, IotHubWirePropertyNames.ContentType},
            {MessageSystemPropertyNames.ContentEncoding, IotHubWirePropertyNames.ContentEncoding},
            {MessageSystemPropertyNames.Operation, MessageSystemPropertyNames.Operation},
            {MessageSystemPropertyNames.Ack, MessageSystemPropertyNames.Ack},
            {MessageSystemPropertyNames.OutputName, IotHubWirePropertyNames.OutputName },
            {MessageSystemPropertyNames.DiagId, IotHubWirePropertyNames.MqttDiagIdKey},
            {MessageSystemPropertyNames.DiagCorrelationContext, IotHubWirePropertyNames.MqttDiagCorrelationContextKey},
            {MessageSystemPropertyNames.InterfaceId, IotHubWirePropertyNames.InterfaceId},
            {MessageSystemPropertyNames.ComponentName,IotHubWirePropertyNames.ComponentName }
        };

        internal MqttTransportHandler(
            IPipelineContext context,
            IotHubConnectionString iotHubConnectionString,
            MqttTransportSettings settings,
            Func<MethodRequestInternal, Task> onMethodCallback = null,
            Action<TwinCollection> onDesiredStatePatchReceivedCallback = null,
            Func<string, Message, Task> onModuleMessageReceivedCallback = null,
            Func<Message, Task> onDeviceMessageReceivedCallback = null)
            : this(context, iotHubConnectionString, settings)
        {
            _methodListener = onMethodCallback;
            _deviceMessageReceivedListener = onDeviceMessageReceivedCallback;
            _moduleMessageReceivedListener = onModuleMessageReceivedCallback;
            _onDesiredStatePatchListener = onDesiredStatePatchReceivedCallback;
        }

        internal MqttTransportHandler(
            IPipelineContext context,
            IotHubConnectionString iotHubConnectionString,
            MqttTransportSettings settings)
            : base(context, settings)
        {
            deviceId = iotHubConnectionString.DeviceId; 
            moduleId = iotHubConnectionString.ModuleId;

            deviceToCloudMessagesTopic = string.Format(CultureInfo.InvariantCulture, DeviceToCloudMessagesTopicFormat, deviceId);

            deviceBoundMessagesReceivingTopic = string.Format(CultureInfo.InvariantCulture, DeviceBoundMessagesReceivingTopicFormat, deviceId);
            deviceBoundMessagesSubscriptionTopic = string.Format(CultureInfo.InvariantCulture, DeviceBoundMessagesSubscriptionTopicFormat, deviceId);

            var mqttFactory = new MqttFactory();

            mqttClient = mqttFactory.CreateMqttClient();
            var mqttClientOptionsBuilder = new MqttClientOptionsBuilder();

            IAuthorizationProvider authorizationProvider = iotHubConnectionString;
            ProductInfo productInfo = context.Get<ProductInfo>();

            if (settings.GetTransportType() == TransportType.Mqtt_WebSocket_Only) //TODO fallbacks?, Proxy?
            {
                var uri = "wss://" + iotHubConnectionString.HostName + "/$iothub/websocket";
                mqttClientOptionsBuilder.WithWebSocketServer(uri);
            }
            else 
            {
                var uri = "ssl://" + iotHubConnectionString.HostName;
                mqttClientOptionsBuilder.WithTcpServer(uri, ProtocolGatewayPort);
            }

            if (iotHubConnectionString.SharedAccessKey != null)
            {
                string clientId = iotHubConnectionString.ModuleId == null ? iotHubConnectionString.DeviceId : iotHubConnectionString.DeviceId + "/" + iotHubConnectionString.ModuleId;
                string username = $"{iotHubConnectionString.HostName}/{clientId}/?{ClientApiVersionHelper.ApiVersionQueryStringLatest}&{DeviceClientTypeParam}={Uri.EscapeDataString(productInfo.ToString())}";
                string password = authorizationProvider.GetPasswordAsync().Result;
                mqttClientOptionsBuilder.WithCredentials(username, password);
                mqttClientOptionsBuilder.WithClientId(clientId);
            }


            mqttClientOptions = mqttClientOptionsBuilder.Build();

            mqttClient.UseApplicationMessageReceivedHandler(HandleReceivedMessage);
            mqttClient.UseDisconnectedHandler(HandleDisconnection);

            isSubscribedToCloudToDeviceMessages = false;
        }

        #region Client operations

        public override async Task OpenAsync(TimeoutHelper timeoutHelper)
        {
            using var cts = new CancellationTokenSource(timeoutHelper.GetRemainingTime());
            await OpenAsync(cts.Token).ConfigureAwait(false);
        }

        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            try
            {
                await mqttClient.ConnectAsync(mqttClientOptions, cancellationToken);
            }
            catch (Exception ex)
            { 
                Console.WriteLine(ex);
                throw;
            }
        }

        public override async Task SendEventAsync(Message message, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var mqttMessage = ComposePublishPacket(message, deviceToCloudMessagesTopic);

            MqttClientPublishResult result = await mqttClient.PublishAsync(mqttMessage, cancellationToken);

            if (result.ReasonCode != MqttClientPublishReasonCode.Success)
            {
                //TODO
                throw new Exception("Failed to publish the mqtt packet");
            }
        }

        public override async Task SendEventAsync(IEnumerable<Message> messages, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // TODO this was the implementation before, but this can be improved since we don't need to wait on a response
            // for each message before sending the next
            foreach (Message message in messages)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await SendEventAsync(message, cancellationToken).ConfigureAwait(false);
            }
        }

        public override async Task<Message> ReceiveAsync(TimeoutHelper timeoutHelper)
        {
            using var cts = new CancellationTokenSource(timeoutHelper.GetRemainingTime());
            return await ReceiveAsync(cts.Token);
        }

        public override async Task<Message> ReceiveAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!isSubscribedToCloudToDeviceMessages)
            {
                MqttClientSubscribeOptions subscribeOptions = new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(deviceBoundMessagesSubscriptionTopic)
                    .Build();

                MqttClientSubscribeResult subscribeResult = await mqttClient.SubscribeAsync(subscribeOptions, cancellationToken);

                if (subscribeResult.Items.Count != 1)
                { 
                    //TODO 
                }

                if (subscribeResult.Items[0].ResultCode != MqttClientSubscribeResultCode.GrantedQoS1)
                { 
                    //TODO
                }

                isSubscribedToCloudToDeviceMessages = true;
            }

            // Wait until either of the linked cancellation tokens have been canceled.
            await _receivingSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

            Message receivedMessage;
            if (!receivedCloudToDeviceMessages.TryDequeue(out receivedMessage))
            { 
                //TODO need to work on semaphore structure. Ideally, 
            }

            return receivedMessage;
        }

        public override async Task EnableMethodsAsync(CancellationToken cancellationToken)
        {
            MqttClientSubscribeOptions subscribeOptions = new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter(DirectMethodsSubscriptionTopicFormat)
                .Build();

            MqttClientSubscribeResult subscribeResult = await mqttClient.SubscribeAsync(subscribeOptions, cancellationToken);

            if (subscribeResult.Items.Count != 1)
            {
                //TODO 
            }

            if (subscribeResult.Items[0].ResultCode != MqttClientSubscribeResultCode.GrantedQoS1)
            {
                //TODO
            }
        }

        public override async Task DisableMethodsAsync(CancellationToken cancellationToken)
        {
            MqttClientUnsubscribeOptions unsubscribeOptions = new MqttClientUnsubscribeOptionsBuilder()
                .WithTopicFilter(DirectMethodsSubscriptionTopicFormat)
                .Build();

            MqttClientUnsubscribeResult unsubscribeResult = await mqttClient.UnsubscribeAsync(unsubscribeOptions, cancellationToken);

            if (unsubscribeResult.Items.Count != 1)
            {
                //TODO 
            }

            if (unsubscribeResult.Items[0].ReasonCode != MqttClientUnsubscribeResultCode.Success)
            {
                //TODO
            }
        }

        public override async Task SendMethodResponseAsync(MethodResponseInternal methodResponse, CancellationToken cancellationToken)
        {
            var topic = MethodResponseTopic.FormatInvariant(methodResponse.Status, methodResponse.RequestId);

            MqttApplicationMessage mqttMessage = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(methodResponse.BodyStream)
                .WithAtLeastOnceQoS()
                .Build();

            MqttClientPublishResult result = await mqttClient.PublishAsync(mqttMessage);

            if (result.ReasonCode != MqttClientPublishReasonCode.Success)
            {
                //TODO
                throw new Exception("Failed to publish the mqtt packet");
            }
        }

        public override Task EnableEventReceiveAsync(bool isAnEdgeModule, CancellationToken cancellationToken)
        {
            throw new NotSupportedException("TODO");
        }

        public override Task DisableEventReceiveAsync(bool isAnEdgeModule, CancellationToken cancellationToken)
        {
            throw new NotSupportedException("TODO");
        }

        public override async Task EnableTwinPatchAsync(CancellationToken cancellationToken)
        {
            //TODO factor out subscribe logic to one function?
            MqttClientSubscribeOptions subscribeOptions = new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter(TwinResponseTopic)
                .Build();

            MqttClientSubscribeResult subscribeResult = await mqttClient.SubscribeAsync(subscribeOptions, cancellationToken);

            if (subscribeResult.Items.Count != 1)
            {
                //TODO 
            }

            if (subscribeResult.Items[0].ResultCode != MqttClientSubscribeResultCode.GrantedQoS1)
            {
                //TODO
            }
        }

        public override async Task DisableTwinPatchAsync(CancellationToken cancellationToken)
        {
            MqttClientUnsubscribeOptions unsubscribeOptions = new MqttClientUnsubscribeOptionsBuilder()
                .WithTopicFilter(TwinResponseTopic)
                .Build();

            MqttClientUnsubscribeResult unsubscribeResult = await mqttClient.UnsubscribeAsync(unsubscribeOptions, cancellationToken);

            if (unsubscribeResult.Items.Count != 1)
            {
                //TODO 
            }

            if (unsubscribeResult.Items[0].ReasonCode != MqttClientUnsubscribeResultCode.Success)
            {
                //TODO
            }
        }

        public override async Task<Twin> SendTwinGetAsync(CancellationToken cancellationToken)
        {
            string rid = Guid.NewGuid().ToString();

            MqttApplicationMessage mqttMessage = new MqttApplicationMessageBuilder()
                .WithTopic(TwinGetTopic.FormatInvariant(rid))
                .WithAtLeastOnceQoS()
                .Build();

            MqttClientPublishResult result = await mqttClient.PublishAsync(mqttMessage, cancellationToken);

            if (result.ReasonCode != MqttClientPublishReasonCode.Success)
            {
                //TODO
                throw new Exception("Failed to publish the mqtt packet");
            }

            //TODO possible that user calls getTwin twice in parallel and this definitely breaks. Talk to team about changing this API a bit in v2
            _getTwinSemaphore.Wait(cancellationToken);

            try
            {
                return receivedTwins[rid];
            }
            catch (Exception ex)
            {
                throw new Exception("TODO", ex);
            }
        }

        public override async Task SendTwinPatchAsync(TwinCollection reportedProperties, CancellationToken cancellationToken)
        {
            string reqId = Guid.NewGuid().ToString();
            string topic = string.Format(TwinReportedPropertiesPatchTopic, reqId);

            string body = JsonConvert.SerializeObject(reportedProperties);

            MqttApplicationMessage mqttMessage = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithAtLeastOnceQoS()
                .WithPayload(Encoding.UTF8.GetBytes(body))
                .Build();

            MqttClientPublishResult result = await mqttClient.PublishAsync(mqttMessage);

            if (result.ReasonCode != MqttClientPublishReasonCode.Success)
            {
                //TODO
                throw new Exception("Failed to publish the mqtt packet");
            }

            _reportedPropertyUpdateResponsesSemaphore.Wait(cancellationToken);

            //TODO try/catch
            int status = reportedPropertyUpdateResponses[reqId];

            if (status != 204)
            {
                throw new Exception("TODO");
            }
        }

        public override async Task CompleteAsync(string lockToken, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            MqttApplicationMessageReceivedEventArgs messageToAcknowledge;
            try
            {
                messageToAcknowledge = messagesToAcknowledge[lockToken];
            }
            catch (Exception ex) //TODO can be more specific with the error caught here
            { 
                throw new Exception("Could not correlate the provided lock token with a received message", ex);
            }

            await messageToAcknowledge.AcknowledgeAsync(cancellationToken);

            messagesToAcknowledge.Remove(lockToken);
        }

        public override Task AbandonAsync(string lockToken, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            throw new NotSupportedException("MQTT protocol does not support this operation");
        }

        public override Task RejectAsync(string lockToken, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            throw new NotSupportedException("MQTT protocol does not support this operation");
        }

        protected override void Dispose(bool disposing)
        {
            mqttClient?.Dispose();
            base.Dispose(disposing);
        }

        public override async Task CloseAsync(CancellationToken cancellationToken)
        {
            await mqttClient.DisconnectAsync(cancellationToken);
        }

        #endregion Client operations

        private Task HandleDisconnection(MqttClientDisconnectedEventArgs disconnectedEventArgs)
        {
            Console.WriteLine("Disconnected");
            return Task.CompletedTask;
        }

        private async Task HandleReceivedMessage(MqttApplicationMessageReceivedEventArgs receivedEventArgs)
        {
            receivedEventArgs.AutoAcknowledge = false;
            string topic = receivedEventArgs.ApplicationMessage.Topic;
            if (topic.StartsWith(deviceBoundMessagesReceivingTopic))
            {
                HandleReceivedCloudToDeviceMessage(receivedEventArgs);
            }
            else if (topic.StartsWith(TwinDesiredPropertiesPatchTopic))
            {
                await HandleReceivedDesiredPropertiesUpdateRequest(receivedEventArgs);
            }
            else if (topic.StartsWith(DirectMethodsReceivingTopicFormat, StringComparison.OrdinalIgnoreCase))
            {
                await HandleReceivedDirectMethodRequest(receivedEventArgs);
            }
            else if (topic.StartsWith(TwinResponseTopic))
            {
                HandleTwinResponse(receivedEventArgs);
            }
            else
            {
                //TODO log warn
                return;
            }
        }

        private void HandleReceivedCloudToDeviceMessage(MqttApplicationMessageReceivedEventArgs receivedEventArgs)
        {
            byte[] payload = receivedEventArgs.ApplicationMessage.Payload;

            //TODO dispose?
#pragma warning disable CA2000 // Dispose objects before losing scope
            var receivedCloudToDeviceMessage = new Message(payload);
#pragma warning restore CA2000 // Dispose objects before losing scope

            PopulateMessagePropertiesFromPacket(receivedCloudToDeviceMessage, receivedEventArgs.ApplicationMessage);
            
            receivedCloudToDeviceMessages.Enqueue(receivedCloudToDeviceMessage);

            _receivingSemaphore.Release();

            // save the received mqtt message instance so that it can be completed later
            messagesToAcknowledge[receivedCloudToDeviceMessage.LockToken] = receivedEventArgs;
        }

        private async Task HandleReceivedDirectMethodRequest(MqttApplicationMessageReceivedEventArgs receivedEventArgs)
        {
            byte[] payload = receivedEventArgs.ApplicationMessage.Payload;

            //TODO dispose?
#pragma warning disable CA2000 // Dispose objects before losing scope
            var receivedDirectMethod = new Message(payload);
#pragma warning restore CA2000 // Dispose objects before losing scope

            PopulateMessagePropertiesFromPacket(receivedDirectMethod, receivedEventArgs.ApplicationMessage);

            // TODO parse out method payload to get name, etc.

            string[] tokens = Regex.Split(receivedEventArgs.ApplicationMessage.Topic, "/", RegexOptions.Compiled);

            using var methodRequest = new MethodRequestInternal(tokens[3], tokens[4].Substring(6), new MemoryStream(receivedEventArgs.ApplicationMessage.Payload), CancellationToken.None);

            //TODO do this on another thread, right? Maybe don't await this?
            await Task.Run(() => _methodListener(methodRequest)).ConfigureAwait(false);

            //TODO here or later?
            receivedEventArgs.AutoAcknowledge = true;
        }

        private async Task HandleReceivedDesiredPropertiesUpdateRequest(MqttApplicationMessageReceivedEventArgs receivedEventArgs)
        {
            byte[] payload = receivedEventArgs.ApplicationMessage.Payload;

            string patch = Encoding.UTF8.GetString(receivedEventArgs.ApplicationMessage.Payload);
            TwinCollection twinCollection = JsonConvert.DeserializeObject<TwinCollection>(patch);

            //TODO do this on another thread, right? Maybe don't await this?
            await Task.Run(() => _onDesiredStatePatchListener.Invoke(twinCollection)).ConfigureAwait(false);

            receivedEventArgs.AutoAcknowledge = true;
        }

        private void HandleTwinResponse(MqttApplicationMessageReceivedEventArgs receivedEventArgs)
        {
            if (ParseResponseTopic(receivedEventArgs.ApplicationMessage.Topic, out string receivedRid, out int status))
            {
                byte[] payload = receivedEventArgs.ApplicationMessage.Payload;
                if (payload.Length == 0)
                {
                    reportedPropertyUpdateResponses[receivedRid] = status;
                    _reportedPropertyUpdateResponsesSemaphore.Release();
                }
                else 
                {
                    string body = Encoding.UTF8.GetString(payload);

                    if (status != 200)
                    {
                        //TODO throw? But not from this thread
                    }

                    try
                    {
                        Twin twin = new Twin
                        {
                            Properties = JsonConvert.DeserializeObject<TwinProperties>(body),
                        };

                        receivedTwins[receivedRid] = twin;
                    }
                    catch (JsonReaderException ex)
                    {
                        if (Logging.IsEnabled)
                            Logging.Error(this, $"Failed to parse Twin JSON: {ex}. Message body: '{body}'");

                        // TODO don't throw in a callback thread
                        throw;
                    }

                    // in a finally block?
                    _getTwinSemaphore.Release();
                }
            }
        }

        //TODO only used in one place?
        public MqttApplicationMessage ComposePublishPacket(Message message, string topicName)
        {
            if (Logging.IsEnabled)
                Logging.Enter(this, topicName, nameof(ComposePublishPacket));

            try
            {
                string TopicName = PopulateMessagePropertiesFromMessage(topicName, message);
                                
                Stream payloadStream = message.GetBodyStream();
                long streamLength = payloadStream.Length;
                if (streamLength > MaxMessageSize)
                {
                    throw new InvalidOperationException($"Message size ({streamLength} bytes) is too big to process. Maximum allowed payload size is {MaxMessageSize}");
                }

                return new MqttApplicationMessageBuilder()
                    .WithTopic(TopicName)
                    .WithPayload(message.GetBodyStream())
                    .WithAtLeastOnceQoS()
                    .Build();
            }
            finally
            {
                if (Logging.IsEnabled)
                    Logging.Exit(this, topicName, nameof(ComposePublishPacket));
            }
        }

        public void PopulateMessagePropertiesFromPacket(Message message, MqttApplicationMessage mqttMessage)
        {
            message.LockToken = (++nextLockToken).ToString();

            // Device bound messages could be in 2 formats, depending on whether it is going to the device, or to a module endpoint
            // Format 1 - going to the device - devices/{deviceId}/messages/devicebound/{properties}/
            // Format 2 - going to module endpoint - devices/{deviceId}/modules/{moduleId/endpoints/{endpointId}/{properties}/
            // So choose the right format to deserialize properties.
            string[] topicSegments = mqttMessage.Topic.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            string propertiesSegment = topicSegments.Length > 6 ? topicSegments[6] : topicSegments[4];

            Dictionary<string, string> properties = UrlEncodedDictionarySerializer.Deserialize(propertiesSegment, 0);
            foreach (KeyValuePair<string, string> property in properties)
            {
                if (s_toSystemPropertiesMap.TryGetValue(property.Key, out string propertyName))
                {
                    message.SystemProperties[propertyName] = ConvertToSystemProperty(property);
                }
                else
                {
                    message.Properties[property.Key] = property.Value;
                }
            }
        }

        private static string ConvertFromSystemProperties(object systemProperty)
        {
            if (systemProperty is string)
            {
                return (string)systemProperty;
            }
            if (systemProperty is DateTime)
            {
                return ((DateTime)systemProperty).ToString("o", CultureInfo.InvariantCulture);
            }
            return systemProperty?.ToString();
        }

        private static object ConvertToSystemProperty(KeyValuePair<string, string> property)
        {
            if (string.IsNullOrEmpty(property.Value))
            {
                return property.Value;
            }
            if (property.Key == IotHubWirePropertyNames.AbsoluteExpiryTime ||
                property.Key == IotHubWirePropertyNames.CreationTimeUtc)
            {
                return DateTime.ParseExact(property.Value, "o", CultureInfo.InvariantCulture);
            }
            if (property.Key == MessageSystemPropertyNames.Ack)
            {
                return Utils.ConvertDeliveryAckTypeFromString(property.Value);
            }
            return property.Value;
        }

        internal static string PopulateMessagePropertiesFromMessage(string topicName, Message message)
        {
            var systemProperties = new Dictionary<string, string>();
            foreach (KeyValuePair<string, object> property in message.SystemProperties)
            {
                if (s_fromSystemPropertiesMap.TryGetValue(property.Key, out string propertyName))
                {
                    systemProperties[propertyName] = ConvertFromSystemProperties(property.Value);
                }
            }
            string properties = UrlEncodedDictionarySerializer.Serialize(Utils.MergeDictionaries(new IDictionary<string, string>[] { systemProperties, message.Properties }));

            string msg = properties.Length != 0
                ? topicName.EndsWith(SegmentSeparator, StringComparison.Ordinal) ? topicName + properties + SegmentSeparator : topicName + SegmentSeparator + properties
                : topicName;
            if (Encoding.UTF8.GetByteCount(msg) > MaxTopicNameLength)
            {
                throw new MessageTooLargeException($"TopicName for MQTT packet cannot be larger than {MaxTopicNameLength} bytes, " +
                    $"current length is {Encoding.UTF8.GetByteCount(msg)}." +
                    $" The probable cause is the list of message.Properties and/or message.systemProperties is too long. " +
                    $"Please use AMQP or HTTP.");
            }

            return msg;
        }

        private bool ParseResponseTopic(string topicName, out string rid, out int status)
        {
            Match match = _twinResponseTopicRegex.Match(topicName);
            if (match.Success)
            {
                status = Convert.ToInt32(match.Groups[1].Value, CultureInfo.InvariantCulture);
                rid = HttpUtility.ParseQueryString(match.Groups[2].Value).Get("$rid");
                return true;
            }

            rid = "";
            status = 500;
            return false;
        }
    }
}
