using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.SignalRService;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Microsoft.InnovateFPGA2021
{
    public static class EventHubConsumer
    {

        private const string _signalr_Hub = "telemetryhub";
        private static ILogger _logger = null;

        //
        // An example of pushing data to Web UI in realtime.
        //
        // This function subscribes Telemetry Event from Event Hubs from 'telemetry-functions-cg' consumer group of 'devicetelemetryhub' Event Hub.
        // It then creates SignalR message (SIGNALR_DATA) to push updates to SignalR Service Hub 'telemetryhub'.
        // Web app (or any apps) subscribing to SignalR updates will receive the notification.
        // Without this, the user must refresh browser view to see new data.
        //
        //
        //  +------------+     +---------+     +------------+     +----------+     +---------+     +--------+ 
        //  | IoT Device | ==> | IoT Hub | <== | Event Hubs | <== | Function | ==> | SignalR | ==> | WebApp | 
        //  +------------+     +---------+     +------------+     +----------+     +---------+     +--------+ 

        [FunctionName("EventHubConsumer")]
        public static async Task Run([EventHubTrigger("devicetelemetryhub",
                                                      ConsumerGroup = "$default",
                                                      Connection = "AzureEventHubsConnectionString")] 
                                                      EventData[] eventData,
                                     [SignalR(HubName = _signalr_Hub)] IAsyncCollector<SignalRMessage> signalRMessage,
                                     ILogger logger)
        {
            var exceptions = new List<Exception>();

            _logger = logger;

            foreach (EventData ed in eventData)
            {
                try
                {
                    if (ed.SystemProperties.ContainsKey("iothub-message-source"))
                    {
                        // look for device id
                        string deviceId = ed.SystemProperties["iothub-connection-device-id"].ToString();
                        string msgSource = ed.SystemProperties["iothub-message-source"].ToString();
                        string signalr_target = string.Empty;
                        string model_id = string.Empty;

                        if (msgSource != "Telemetry")
                        {
                            _logger.LogInformation($"IoT Hub Message Source {msgSource}");
                        }

                        _logger.LogInformation($"Telemetry Message : {Encoding.UTF8.GetString(ed.Body.Array, ed.Body.Offset, ed.Body.Count)}");

                        DateTime enqueuTime = (DateTime)ed.SystemProperties["iothub-enqueuedtime"];

                        // look for IoT Plug and Play model id (DTMI)
                        if (ed.SystemProperties.ContainsKey("dt-dataschema"))
                        {
                            model_id = ed.SystemProperties["dt-dataschema"].ToString();
                            _logger.LogInformation($"IoT Plug and Play Model ID {model_id}");
                            // You may parse IoT Plug and Play model here to perform device specific operations.
                        }

                        // Initialize SignalR Data
                        SIGNALR_DATA signalrData = new SIGNALR_DATA
                        {
                            eventId = ed.SystemProperties["x-opt-sequence-number"].ToString(),
                            eventType = "Event Hubs",
                            eventTime = enqueuTime.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'"),
                            eventSource = msgSource,
                            deviceId = deviceId,
                            dtDataSchema = model_id,
                            data = null
                        };

                        // Process telemetry based on message source
                        switch (msgSource)
                        {
                            case "Telemetry":
                                OnTelemetryReceived(signalrData, ed);
                                signalr_target = "DeviceTelemetry";
                                break;
                            case "twinChangeEvents":
                                OnDeviceTwinChanged(signalrData, ed);
                                signalr_target = "DeviceTwinChange";
                                break;
                            case "digitalTwinChangeEvents":
                                OnDigitalTwinTwinChanged(signalrData, ed);
                                signalr_target = "DigitalTwinChange";
                                break;
                            case "deviceLifecycleEvents":
                                OnDeviceLifecycleChanged(signalrData, ed);
                                signalr_target = "DeviceLifecycle";
                                break;
                            default:
                                break;
                        }

                        if (signalrData.data != null)
                        {
                            // send to SignalR Hub
                            var data = JsonConvert.SerializeObject(signalrData);

                            await signalRMessage.AddAsync(new SignalRMessage
                            {
                                Target = signalr_target,
                                Arguments = new[] { data }
                            });
                        }

                        signalrData = null;
                    }
                    else
                    {
                        _logger.LogInformation("Unsupported Message Source");
                    }
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                }
            }

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

        private static void OnTelemetryReceived(SIGNALR_DATA signalrData, EventData eventData)
        {
            _logger.LogInformation($"{System.Reflection.MethodBase.GetCurrentMethod().Name}");
            signalrData.data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
        }

        // Process Device Twin Change Event
        // Add filtering etc as needed
        // leave signalrData.data to null if we do not want to send SignalR message
        private static void OnDeviceTwinChanged(SIGNALR_DATA signalrData, EventData eventData)
        {
            _logger.LogInformation($"{System.Reflection.MethodBase.GetCurrentMethod().Name}");
            signalrData.data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
        }

        // Process Digital Twin Change Event Telemetry
        // Add filtering etc as needed
        // leave signalrData.data to null if we do not want to send SignalR message
        private static void OnDigitalTwinTwinChanged(SIGNALR_DATA signalrData, EventData eventData)
        {
            _logger.LogInformation($"{System.Reflection.MethodBase.GetCurrentMethod().Name}");
            signalrData.data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
        }

        // Process Device Lifecycle Change event Telemetry
        // Add filtering etc as needed
        // leave signalrData.data to null if we do not want to send SignalR message
        private static void OnDeviceLifecycleChanged(SIGNALR_DATA signalrData, EventData eventData)
        {
            _logger.LogInformation($"{System.Reflection.MethodBase.GetCurrentMethod().Name}");
            signalrData.data = JsonConvert.SerializeObject(eventData.Properties);
        }

        public class SIGNALR_DATA
        {
            public string eventId { get; set; }
            public string eventType { get; set; }
            public string deviceId { get; set; }
            public string eventSource { get; set; }
            public string eventTime { get; set; }
            public string data { get; set; }
            public string dtDataSchema { get; set; }
        }
    }
}
