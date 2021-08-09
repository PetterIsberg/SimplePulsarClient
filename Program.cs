using DotPulsar;
using DotPulsar.Extensions;
using System;
using System.Buffers;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimplePulsarConsumer
{
   static class Program
   {
      public static async Task Main()
      {
         var client = PulsarClient
            .Builder()
            .ExceptionHandler(CustomExceptionHandler)
            .Build();
         var consumer = client.NewConsumer()
            .StateChangedHandler(Monitor)
            .SubscriptionName("MySubscription")
            .Topic("persistent://public/default/mytopic")
            .Create();

         while (true)
         {
            var message = await consumer.Receive().ConfigureAwait(false);
            Console.WriteLine("Received: " + Encoding.UTF8.GetString(message.Data.ToArray()));
            await consumer.Acknowledge(message).ConfigureAwait(false);
         }
      }

      private static void Monitor(ConsumerStateChanged stateChanged, CancellationToken cancellationToken)
      {
         var stateMessage = stateChanged.ConsumerState switch
         {
            ConsumerState.Active => "is active",
            ConsumerState.Inactive => "is inactive",
            ConsumerState.Disconnected => "is disconnected",
            ConsumerState.Closed => "has closed",
            ConsumerState.ReachedEndOfTopic => "has reached end of topic",
            ConsumerState.Faulted => "has faulted",
            _ => $"has an unknown state '{stateChanged.ConsumerState}'"
         };

         var topic = stateChanged.Consumer.Topic;
         Console.WriteLine($"The consumer for topic '{topic}' " + stateMessage);
      }

      private static async ValueTask CustomExceptionHandler(ExceptionContext exceptionContext)
      {
         if (exceptionContext.Exception is SocketException socketException)
         {
            Console.WriteLine($"Pulsar client is disconnected: {socketException}");
            await Task.Delay(TimeSpan.FromSeconds(5), exceptionContext.CancellationToken).ConfigureAwait(false);
            exceptionContext.Result = FaultAction.Retry;
            exceptionContext.ExceptionHandled = true;
         }
         else
            Console.WriteLine($"Pulsar client exception: {exceptionContext.Exception}");
      }
   }
}
