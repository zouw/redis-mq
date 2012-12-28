#ifndef MQ_SERVICE_ICE
#define MQ_SERVICE_ICE

module com{
	module renren {
		module x2 {
			module mq {
				interface MqService {
					   /*
						 发布者发送消息到MQ
					   */
					   int sendMq(int mid, string msg);
					   
					   /*
						 被MQ调用，推送消息到订阅者
					   */
				   	   int forwardMq(int mid, string msg);
				};
			};
		};
	};
};


#endif
