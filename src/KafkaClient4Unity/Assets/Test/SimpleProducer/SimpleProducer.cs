﻿// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
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

namespace Confluent.Kafka.Examples.SimpleProducer
{
    public class SimpleProducer : MonoBehaviour
    {
        public void Start()
        {
            string brokerList = "brokerListExample";
            string topicName = "topicNameExample";

            var config = new Dictionary<string, object> { { "bootstrap.servers", brokerList } };

            using (var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8)))
            {
                Debug.Log($"{producer.Name} producing on {topicName}. q to exit.");

                string text = "testMessageFromHighskillz";
                //while ((text = Console.ReadLine()) != "q")
                //{
                    var deliveryReport = producer.ProduceAsync(topicName, null, text);
                    deliveryReport.ContinueWith(task =>
                    {
                        Debug.Log($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                    });
                //}

                // Tasks are not waited on synchronously (ContinueWith is not synchronous),
                // so it's possible they may still in progress here.
                producer.Flush(TimeSpan.FromSeconds(10));
            }
        }
    }
}
