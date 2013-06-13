#!/usr/bin/python

"""
Wrapper around Charniak's parser, allowing to use it one sentence at a time
while the engine is running. It parallelizes parsing by using a pool of parser 
processes in the background. More importantly, it gracefully handles inputs 
for which the Charniak parser crashes or hangs.

Usage:
1) As a library: call init() first, then parse_sentence(txt) for each sentence.
   At teardown, you might want kill_parsers()
2) As a standalone web service: just run it (port and hostname hardcoded below);
   for every line of input that you send to the socket, you receive a line of output,
   i.e. the parsed sentence.
"""
import sys, os

# config
try:    CURRENT_DIR = os.path.split(os.path.abspath(__file__))[0]
except: CURRENT_DIR = '/home/mitjat/srl/charniak/'
NUM_PARSERS = 2 # number of parser processes running in the background
PARSER_DIR = CURRENT_DIR+'/reranking-parser/'
PARSER_CMD = ['./parse.sh', 'STDIN']

HOST = 'localhost' # if run as a standalone script
PORT = 21568



import sys, os; sys.path.extend(('.','..'))

from subprocess import Popen, PIPE
from Queue import Queue, Empty

from collections import defaultdict
from socket import *
import thread, threading, select
import time
import re
import traceback

try:
	import logger
	log = logger.Logger(verbosity=100)
except:
	print 'WARNING - using fallback logging'
	def log(*msg):
		print ' '.join(map(str,msg[1:]))

parser_pool = [] # contains instances of ParserControlThread
job_queue = Queue()    # Contains tuples (idx, sentence_text, result_queue) for sentences to be parsed.
	
#sys.stdout = os.fdopen(sys.__stdout__.fileno(), 'w', 0) # disable buffering

def readline_timeout(f, timeout):
	'''
	Read a line from `f`; if nothing is available after `timeout` seconds, return None.
	WARNING: makes the crude assumption that as soon as a single byte is available in f,
	a whole line will be available. Violation of assumption leads to an unlimited wait.
	'''
	rlist, wlist, xlist = select.select([f],[],[],timeout)
	if not rlist: return None
	else: return f.readline()

def proc_subtree(pid0):
	'''
	Return a list of all subprocesses of `pid0`.
	'''
	# get process listing
	ch = defaultdict(list) # ppid -> [children pids]
	lines = Popen('ps axo ppid,pid'.split(), stdout=PIPE).communicate()[0].splitlines()
	for line in lines:
		ppid, pid = line.split()
		if not (pid.isdigit() and ppid.isdigit()): continue
		ch[int(ppid)].append(int(pid))

	# reconstruct the subtree
	result = [pid0]
	done = 0
	while done < len(result):
		result.extend(ch[result[done]])
		done += 1
	return result
	
class ParserControlThread(threading.Thread):
	'A simple program driving a parser subprocess, taking care of hangups/timeouts.'
	def __init__(self):
		self.activeJob = None 
		self.proc = None
		threading.Thread.__init__(self)

	def _restartProc(self):
		'Kill the existing parser process.'
		log(4, 'Killing active parser process.')
		if self.proc==None:
			log(5, 'Kill unnecessary')
		else:
			try:
				for pid in proc_subtree(self.proc.pid): os.kill(pid, 9)
				log(5, 'Kill successful')
			except:
				log(5, 'Kill failed: '+traceback.format_exc())

		# will cause _ensureProc to start a new process. Do
		# not create it here as this func is not necessarily
		# called from within the thread
		self.proc = None 

	def _ensureProc(self):
		'''
		Make sure that self.proc is a running (but maybe blocked -- cannot say) process
		If needed, create a new parser subprocess, store the reference in self.proc
		'''
		if not self.proc:
			log(3, 'Starting a parser process ...')
			proc = Popen(PARSER_CMD, cwd=PARSER_DIR, stdin=PIPE, stdout=PIPE, shell=True)
			proc.stdin.write('<s> asdf1 </s>\n') # you have to write and read in exactly this succession
			proc.stdin.write('<s> asdf2 </s>\n')
			log(4, 'Parser loading ...')
			proc.stdout.readline()
			log(3, 'Parser started.')
			self.proc = proc
		else:
			log(5, 'Keeping the existing parser: %r, PID %r, return code %r' % (self.proc, self.proc.pid, self.proc.returncode))

	def run(self):
		self._restartProc()
		
		while True:
			self._ensureProc()
			self.activeJob = job_queue.get()
			idx, line, result_queue = self.activeJob
			result = '' # fallback
			log(6,'Starting job, qsize is', result_queue.qsize(), self.activeJob)

			# clean the types of input on which we know charniak chokes
			input_ok = True
			line = line.replace('</s>', '</ s>')
			if isinstance(line, unicode): line = line.encode('utf8','replace')
			if not re.search('[a-zA-Z0-9]', line):
				result = ''
				input_ok = False

			if input_ok:
				try:
					log(4, 'Parsing %r' % line)

					self.proc.stdin.write('<s> %s </s>\n' % line)
					self.proc.stdin.flush()
					bogus = readline_timeout(self.proc.stdout, 10) # bogus output, see README-mitja file for charniak
					#print 'Bogus is %r' % bogus
					if bogus==None: raise RuntimeError
					self.proc.stdin.write('<s> bloh </s>\n')
					result = readline_timeout(self.proc.stdout, 10)
					if result==None: raise RuntimeError
					result = result.decode('utf8','replace')
					log(4, 'Result is %r' % result)
				except IOError:
					# broken pipe - this parser process is useless
					log(2, 'Broken pipe to charniak process. Restarting parser.')
					result = ''
					self._restartProc()
				except RuntimeError:
					# broken pipe - this parser process is useless
					log(3, 'Parse timed out. Restaring parser. Problematic job:', self.activeJob)
					result = ''
					self._restartProc()

			result_queue.put((idx,result))
			log(6,'Finished job, qsize is', result_queue.qsize(), self.activeJob)
			self.activeJob = None

				
def conn_handler(clientsock, addr):
	clientfile = clientsock.makefile('rb')

	for line in clientfile:
		result = parse_sentence(line)
		clientsock.send(result)

	clientsock.close()

def parse_sentence(line):
	assert type(line) in (str, unicode)
	return parse_sentences([line])[0]

def parse_sentences(lines):
	'''
	Parse a list of sentences; returns a list of parses, each in lispish notation.
	'''
	assert type(lines) in (list, tuple)
	if not lines:
		return []
	
	# Create jobs
	result_queue = Queue(len(lines))
	for idx,line in enumerate(lines):
		job_queue.put((idx,line,result_queue))
		log(5, 'Equeued', (idx,line,result_queue))

	while not result_queue.full(): time.sleep(.05)
	
	# collect results
	result = dict(result_queue.get() for i in range(result_queue.qsize()))
	return [result.get(idx,'') for idx in range(len(lines))]
	

def init(num_parsers=NUM_PARSERS,parser_cmd=PARSER_CMD,parser_dir=PARSER_DIR):
	'Start num_parsers parser processes in the background.'
	global NUM_PARSERS, PARSER_CMD, PARSER_DIR
	NUM_PARSERS = num_parsers
	PARSER_CMD = parser_cmd
	PARSER_DIR = parser_dir
	while len(parser_pool) < num_parsers:
		t = ParserControlThread()
		t.daemon = True
		parser_pool.append(t)
		t.start()
	log(4, '%d parser control threads started' % num_parsers)

def kill_parsers():
	global parser_pool
	for ppool in parser_pool:
		try:
			for pid in proc_subtree(ppool.proc.pid):
				os.kill(pid,9)
		except Empty: break
	parser_pool = []

def main():
	'Set up the multithreaded web server'
	global serversock
	try:
		serversock = socket(AF_INET, SOCK_STREAM)
		serversock.bind((HOST, PORT))
		serversock.listen(2)

		init()
		
		while 1:
			print 'waiting for connection ...'
			clientsock, addr = serversock.accept()
			print '... connected from:', addr
			thread.start_new_thread(conn_handler, (clientsock, addr))
			#conn_handler(clientsock,addr)
	finally:
		try:
			serversock.close()
			kill_parsers()
		except:
			print 'NOTE: teardown failed'
			import traceback
			traceback.print_exc()
		
				
if __name__=='__main__':
	if 1:
		main()
	else:
		# short self-test
		init()
		print parse_sentences(['Who let the dogs out?', '! foo ! ! . <>', 'I have no clue']*3)
