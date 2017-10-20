#pylint: disable=C0103,W0105,broad-except,logging-not-lazy

"""
Here's the algorithm

1. get active users
2. choose N users where N is from the config
3. create a multiprocessing Pool of size N
4. spawn a process per task that publish their files
"""
import logging
import os
import multiprocessing
import subprocess
from multiprocessing import Pool
from MultiProcessingLog import MultiProcessingLog
from logging.handlers import TimedRotatingFileHandler

from WMCore.WMFactory import WMFactory
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Configuration import loadConfigurationFile

from RESTInteractions import HTTPRequests
from ServerUtilities import encodeRequest, oracleOutputMapping


class Worker(object):
    """

    """
    def __init__(self, config, quiet):
        """
        Initialise class members
        """
        self.config = config.General
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

        self.logger = setRootLogger(quiet, True)

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

        self.logger.debug('kicking off pool %s' % [x[0][3] for x in tasks])

        self.pool.map(self.startSlave, tasks)


    def startSlave(task):
        # - process logger
        logger = setProcessLogger(str(task))
        logger.info("Process %s is starting. PID %s", procnum, os.getpid())

        self.force_publication = False
        workflow = str(task[0][3])
        wfnamemsg = "%s: " % (workflow)

        if len(task[1]) > self.max_files_per_block:
            self.force_publication = True
            msg = "All datasets have more than %s ready files." % (self.max_files_per_block)
            msg += " No need to retrieve task status nor last publication time."
            logger.info(wfnamemsg+msg)
        else:
            msg  = "At least one dataset has less than %s ready files." % (self.max_files_per_block)
            logger.info(wfnamemsg+msg)
            # Retrieve the workflow status. If the status can not be retrieved, continue
            # with the next workflow.
            workflow_status = ''
            url = '/'.join(self.cache_area.split('/')[:-1]) + '/workflow'
            msg = "Retrieving status from %s" % (url)
            logger.info(wfnamemsg+msg)
            buf = cStringIO.StringIO()
            header = {"Content-Type ":"application/json"}
            try:
                _, res_ = self.connection.request(url,
                                                  data,
                                                  header,
                                                  doseq=True,
                                                  ckey=self.userProxy,
                                                  cert=self.userProxy
                                                 )# , verbose=True) #  for debug
            except Exception as ex:
                if self.config.isOracle:
                    logger.exception('Error retrieving status from cache.')
                    return 
                else:
                    msg = "Error retrieving status from cache. Fall back to user cache area"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    logger.error(wfnamemsg+msg)
                    query = {'key': self.user}
                    try:
                        self.user_cache_area = self.db.loadView('DBSPublisher', 'cache_area', query)['rows']
                    except Exception as ex:
                        msg = "Error getting user cache_area"
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        logger.error(msg)

                    self.cache_area = self.user_cache_area[0]['value'][0]+self.user_cache_area[0]['value'][1]+'/filemetadata'
                    try:
                        _, res_ = self.connection.request(url,
                                                          data,
                                                          header,
                                                          doseq=True,
                                                          ckey=self.userProxy,
                                                          cert=self.userProxy
                                                     )#, verbose=True)# for debug
                    except Exception as ex:
                        msg = "Error retrieving status from user cache area."
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        logger.error(wfnamemsg+msg)
            msg = "Status retrieved from cache. Loading task status."
            logger.info(wfnamemsg+msg)
            try:
                buf.close()
                res = json.loads(res_)
                workflow_status = res['result'][0]['status']
                msg = "Task status is %s." % workflow_status
                logger.info(wfnamemsg+msg)
            except ValueError:
                msg = "Workflow removed from WM."
                logger.error(wfnamemsg+msg)
                workflow_status = 'REMOVED'
            except Exception as ex:
                msg = "Error loading task status!"
                msg += str(ex)
                msg += str(traceback.format_exc())
                logger.error(wfnamemsg+msg)
            # If the workflow status is terminal, go ahead and publish all the ready files
            # in the workflow.
            if workflow_status in ['COMPLETED', 'FAILED', 'KILLED', 'REMOVED']:
                self.force_publication = True
                if workflow_status in ['KILLED', 'REMOVED']:
                    self.force_failure = True
                msg = "Considering task status as terminal. Will force publication."
                logger.info(wfnamemsg+msg)
            # Otherwise...
            else:
                msg = "Task status is not considered terminal."
                logger.info(wfnamemsg+msg)
                msg = "Getting last publication time."
                logger.info(wfnamemsg+msg)
                # Get when was the last time a publication was done for this workflow (this
                # should be more or less independent of the output dataset in case there are
                # more than one).
                last_publication_time = None
                if not self.config.isOracle:
                    query = {'reduce': True, 'key': user_wf['key']}
                    try:
                        last_publication_time = self.db.loadView('DBSPublisher', 'last_publication', query)['rows']
                    except Exception as ex:
                        msg = "Cannot get last publication time for %s: %s" % (user_wf['key'], ex)
                        logger.error(wfnamemsg+msg)
                else:
                    data['workflow'] = workflow
                    data['subresource'] = 'search'
                    try:
                        result = self.oracleDB.get(self.config.oracleFileTrans.replace('filetransfers','task'),
                                                   data=encodeRequest(data))
                        logger.debug("task: %s " %  str(result[0]))
                        logger.debug("task: %s " %  getColumn(result[0],'tm_last_publication'))
                    except Exception as ex:
                        logger.error("Error during task doc retrieving: %s" %ex)
                    if last_publication_time:
                        date = oracleOutputMapping(result)['last_publication']
                        seconds = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f").timetuple()
                        last_publication_time = time.mktime(seconds)

                msg = "Last publication time: %s." % str(last_publication_time)
                logger.debug(wfnamemsg+msg)
                # If this is the first time a publication would be done for this workflow, go
                # ahead and publish.
                if not last_publication_time:
                    self.force_publication = True
                    msg = "There was no previous publication. Will force publication."
                    logger.info(wfnamemsg+msg)
                # Otherwise...
                else:
                    last = last_publication_time
                    msg = "Last published block: %s" % (last)
                    logger.debug(wfnamemsg+msg)
                    # If the last publication was long time ago (> our block publication timeout),
                    # go ahead and publish.
                    time_since_last_publication = now - last
                    hours = int(time_since_last_publication/60/60)
                    minutes = int((time_since_last_publication - hours*60*60)/60)
                    timeout_hours = int(self.block_publication_timeout/60/60)
                    timeout_minutes = int((self.block_publication_timeout - timeout_hours*60*60)/60)
                    msg = "Last publication was %sh:%sm ago" % (hours, minutes)
                    if time_since_last_publication > self.block_publication_timeout:
                        self.force_publication = True
                        msg += " (more than the timeout of %sh:%sm)." % (timeout_hours, timeout_minutes)
                        msg += " Will force publication."
                    else:
                        msg += " (less than the timeout of %sh:%sm)." % (timeout_hours, timeout_minutes)
                        msg += " Not enough to force publication."
                    logger.info(wfnamemsg+msg)

            if self.force_publication:
                # - get info
                active_ = [{'key': [x['username'],
                                    x['user_group'],
                                    x['user_role'],
                                    x['taskname']],
                            'value': [x['destination'],
                                      x['source_lfn'],
                                      x['destination_lfn'],
                                      x['input_dataset'],
                                      x['dbs_url'],
                                      x['last_update']
                                     ]}
                           for x in task[1] if x['transfer_state']==3 and x['publication_state'] not in [2,3,5]]
               
                logger.debug(active_)

                lfn_ready = {}
                wf_jobs_endtime = []
                pnn, input_dataset, input_dbs_url = "", "", ""
                for active_file in active_:
                    job_end_time = active_file['value'][5]
                    if job_end_time and self.config.isOracle:
                        wf_jobs_endtime.append(int(job_end_time) - time.timezone)
                    elif job_end_time:
                        wf_jobs_endtime.append(int(time.mktime(time.strptime(str(job_end_time), '%Y-%m-%d %H:%M:%S'))) - time.timezone)
                    source_lfn = active_file['value'][1]
                    dest_lfn = active_file['value'][2]
                    self.lfn_map[dest_lfn] = source_lfn
                    if not pnn or not input_dataset or not input_dbs_url:
                        pnn = str(active_file['value'][0])
                        input_dataset = str(active_file['value'][3])
                        input_dbs_url = str(active_file['value'][4])
                    filename = os.path.basename(dest_lfn)
                    left_piece, jobid_fileext = filename.rsplit('_', 1)
                    if '.' in jobid_fileext:
                        fileext = jobid_fileext.rsplit('.', 1)[-1]
                        orig_filename = left_piece + '.' + fileext
                    else:
                        orig_filename = left_piece
                    lfn_ready.setdefault(orig_filename, []).append(dest_lfn)

                logger.info("%s %s %s %s %s" % (workflow, input_dataset, input_dbs_url, pnn, lfn_ready))    
                
                userDN = ''
                logger.debug("Trying to get DN")
                try:
                    userDN = getDNFromUserName((task[0],task[1],task[2]), logger)
                except Exception as ex:
                    msg = "Error retrieving the user DN"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    logger.error(msg)
                    init = False
                    return 1

                # - dump info file {UserDN,User,Destination=pnn, task[1]}
                logger.info("%s %s %s %s" % (task[0], userDN, pnn, task[1]))
        # POpen
        #subprocess.call(["publisher.sh", user, userDN, dumpPath])
        return 0


if __name__ == '__main__':

    configuration = loadConfigurationFile( os.path.abspath('config.py') )
    master = Worker(configuration, False)
    master.algorithm()
