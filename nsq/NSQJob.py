
import tornado.httpclient
import tornado.ioloop
import time
import logging
try:
    import simplejson as json
except ImportError:
    import json # pyflakes.ignore


from NSQReader import Reader


class Job(Reader):
    """
    This is a helper job to start a job task using nsqjobtracker
    
    It will get job information from the job tracker, and start a reader to do the work
    """
    def __init__(self, worker_url, **kwargs):
        assert worker_url.startswith("http://") or worker_url.startswith("https://")
        self.http_client = tornado.httpclient.AsyncHTTPClient()
        self.worker_url = worker_url
        self.reader_kwargs = kwargs
        tornado.ioloop.IOLoop.instance().add_timeout(time.time(), self.start_worker)
    
    def start_worker(self):
        req = tornado.httpclient.HTTPRequest(self.worker_url, 
                    method="GET",
                    connect_timeout=2, 
                    request_timeout=5)
        self.http_client.fetch(req, callback=self.finish_start_worker)
    
    def finish_start_worker(self, response):
        if response.error:
            logging.warning("[%s] lookupd error %s", response.request_uri, response.error)
            return
        
        try:
            data = json.loads(response.body)
        except json.JSONDecodeError:
            logging.warning("[%s] failed to parse JSON from lookupd: %r", response.request_uri, response.body)
            return
        
        if data['status_code'] != 200:
            raise Exception("Unable to fetch job information from %s" % self.worker_url)

        logging.info(data)
        self.job_info = data["data"]["job"]
        self.nsqd_http_addresses = data["data"]["nsqd_"]
        
        tasks = {"task":self.handle_message}
        topic = self.job_info["topics"][0]
        channel = "%s-%s" % (self.job_info["nsq_prefix"], self.job_info["timeframe"])
        self.reader_kwargs["nsqd_tcp_addresses"] = data["data"]["nsqd_tcp_addresses"]
        self.reader_kwargs["lookupd_http_addresses"] = data["data"]["lookupd_http_addresses"]
        super(Job, self).__init__(tasks, topic, channel, **self.reader_kwargs)
        # start a poll loop so we know when to stop
    
    def handle_message(self, message):
        raise NotImplemented
    
    
    