// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using UnityEngine;
using System.Collections;

namespace Confluent.Kafka.Examples.SimpleProducer
{
    public class SimpleProducer : MonoBehaviour
    {
        public void Start()
        {
            StartCoroutine(SendTestMessage());
        }

        private IEnumerator SendTestMessage()
        {
            string topicName = "test";

            var config = new Dictionary<string, object> {   { "bootstrap.servers", "52.35.61.218:9092" },
                                                            //{ "builtin.features", "sasl_plain" },
                                                            { "metadata.request.timeout.ms", 5000 },
                                                            { "socket.timeout.ms", 5000 },
                                                            { "sasl.username", "sintef" },
                                                            { "sasl.password", "s3d4f5g" },
                                                            { "client.id", "partner1" },
                                                            { "group.id", "human" },
                                                            { "debug", "broker" },
                                                        };

            using (var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8)))
            {
                //Debug.Log($"{producer.Name} producing on {topicName}. q to exit.");

                string text = "testMessageFromHighskillz";
                //while ((text = Console.ReadLine()) != "q")
                //{

                yield return new WaitForSeconds(10);

                var deliveryReport = producer.ProduceAsync(topicName, null, text);
                deliveryReport.ContinueWith(task =>
                {
                    Debug.Log($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                });
                //}

                yield return new WaitForSeconds(10);

                // Tasks are not waited on synchronously (ContinueWith is not synchronous),
                // so it's possible they may still in progress here.
                //producer.Flush(TimeSpan.FromSeconds(10));
            }

        }
    }
}
