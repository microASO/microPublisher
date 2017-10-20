#pylint: disable=C0103,W0105,broad-except,logging-not-lazy

"""
Here's the algorithm

1. get active users
2. choose N users where N is from the config
3. create a multiprocessing Pool of size N
4. spawn a process per task that publish their files
"""
import logging
import multiprocessing
import subprocess
from multiprocessing import Pool

from WMCore.WMFactory import WMFactory
from WMCore.Database.CMSCouch import CouchServer

from AsyncStageOut.BaseDaemon import BaseDaemon
from AsyncStageOut.PublisherWorker import PublisherWorker

from RESTInteractions import HTTPRequests
from ServerUtilities import encodeRequest, oracleOutputMapping


class Worker(object):
    """

    """
    def __init__(self, config):
        """
        Initialise class members
        """
        #TODO: logger!
        def createLogdir(dirname):
            """ Create the directory dirname ignoring erors in case it exists. Exit if
                the directory cannot be created.
            """
            try:
                os.mkdir(dirname)
            except OSError as ose:
                if ose.errno != 17: #ignore the "Directory already exists error"
                    print(str(ose))
                    print("The task worker need to access the '%s' directory" % dirname)
                    sys.exit(1)


        def setRootLogger(quiet, debug):
            """Sets the root logger with the desired verbosity level
               The root logger logs to logs/twlog.txt and every single
               logging instruction is propagated to it (not really nice
               to read)

            :arg bool quiet: it tells if a quiet logger is needed
            :arg bool debug: it tells if needs a verbose logger
            :return logger: a logger with the appropriate logger level."""

            createLogdir('logs')
            createLogdir('logs/processes')
            createLogdir('logs/tasks')

            logHandler = MultiProcessingLog('logs/log.txt', when='midnight')
            logFormatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s,%(lineno)d:%(message)s")
            logHandler.setFormatter(logFormatter)
            logging.getLogger().addHandler(logHandler)
            loglevel = logging.INFO
            if quiet:
                loglevel = logging.WARNING
            if debug:
                loglevel = logging.DEBUG
            logging.getLogger().setLevel(loglevel)
            logger = setProcessLogger("master")
            logger.debug("PID %s.", os.getpid())
            logger.debug("Logging level initialized to %s.", loglevel)
            return logger

        def setProcessLogger(name):
            """ Set the logger for a single process. The file used for it is logs/processes/proc.name.txt and it
                can be retrieved with logging.getLogger(name) in other parts of the code
            """
            logger = logging.getLogger(name)
            handler = TimedRotatingFileHandler('logs/processes/proc.c3id_%s.pid_%s.txt' % (name, os.getpid()), 'midnight', backupCount=30)
            formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)

    return logger

        self.logger = setRootLogger(quiet, debug)

        try:
            self.oracleDB = HTTPRequests(self.config.oracleDB,
                                         self.config.opsProxy,
                                         self.config.opsProxy)
            self.logger.debug('Contacting OracleDB:' + self.config.oracleDB)
        except:
            self.logger.exception('Failed when contacting Oracle')
            raise

        self.pool = Pool(processes=4)

    def active_tasks(self, db):
        active_users = []

        fileDoc = {}
        fileDoc['asoworker'] = self.config.asoworker
        fileDoc['subresource'] = 'acquirePublication'

        self.logger.debug("Retrieving publications from oracleDB")

        results = ''
        try:
            results = db.post(self.config.oracleFileTrans,
                                data=encodeRequest(fileDoc))
        except Exception as ex:
            self.logger.error("Failed to acquire publications \
                                from oracleDB: %s" %ex)
            return []
            
        fileDoc = dict()
        fileDoc['asoworker'] = self.config.asoworker
        fileDoc['subresource'] = 'acquiredPublication'
        fileDoc['grouping'] = 0
        fileDoc['limit'] = 100000

        self.logger.debug("Retrieving max.100000 acquired puclications from oracleDB")

        result = []

        try:
            results = db.get(self.config.oracleFileTrans,
                                data=encodeRequest(fileDoc))
            result.extend(oracleOutputMapping(results))
        except Exception as ex:
            self.logger.error("Failed to acquire publications \
                                from oracleDB: %s" %ex)
            return []

        self.logger.debug("publen: %s" % len(result))

        self.logger.debug("%s acquired puclications retrieved" % len(result))
        #TODO: join query for publisher (same of submitter)
        unique_tasks = [list(i) for i in set(tuple([x['username'], 
                                                    x['user_group'], 
                                                    x['user_role'], 
                                                    x['taskname']]
                                                    ) for x in result if x['transfer_state'] == 3)]

        info = []
        for task in unique_tasks:
            info.append([x for x in result if x['taskname']==task[3]])
        return zip(unique_tasks, info)

    def algorithm(self, parameters=None):
        """
        1. Get a list of users with files to publish from the couchdb instance
        2. For each user get a suitably sized input for publish
        3. Submit the publish to a subprocess
        """
        tasks = self.active_tasks(self.oracleDB)

        self.logger.debug('kicking off pool %s' % [x[3] for x in tasks])

        self.pool.map(self.startSlave, tasks)


    def startSlave(task):
        # - process logger
        logger = setProcessLogger(str(task))
        logger.info("Process %s is starting. PID %s", procnum, os.getpid())
        logger,

        # - get info

        

        # - dump info file
        # POpen
        #subprocess.call(["publisher.sh", user, userDN, dumpPath])
        pass


if __name__ == '__main__':

    configuration = loadConfigurationFile( os.path.abspath('config.py') )
    master = Worker(configuration)