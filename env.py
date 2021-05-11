import numpy as np

# from park import core, spaces, logger
from param import config

class ReplicaplacementEnv(object):

    def __init__(self):

        self.setup_space()
        self.n_actions = len(self.action_space)
        self.n_features = 2
        self.num_stream_jobs = config.num_stream_jobs
        self.servers = self.initialize_servers()
        self.reset()

    def initialize_servers(self):
        servers = [0] * config.num_servers
        return servers

    def observe(self):
        # obs_arr = []
        # # load on each server
        # for server in self.servers:
        #     obs_arr.append(server)
        # obs_arr = np.array(obs_arr)
        self.observation_space = self.servers
        return self.servers

    def reset(self):
        # for server in self.servers:
        #     server.reset()
        self.servers = self.initialize_servers()
        self.num_stream_jobs_left = self.num_stream_jobs
        assert self.num_stream_jobs_left > 0
        return self.observe()


    def setup_space(self):
        # Set up the observation and action space
        # The boundary of the space may change if the dynamics is changed
        # a warning message will show up every time e.g., the observation falls
        # out of the observation space
        self.observation_space =  [0] * config.num_servers
        self.action_space = range(config.num_servers)
        

    def step(self, action):

        # 0 <= action < num_servers
        # assert self.action_space.contains(action)
        self.servers[action] = self.servers[action] + 1
        reward = 0
        reward -= np.std(self.servers)

        self.num_stream_jobs_left = self.num_stream_jobs_left - 1
        done = (self.num_stream_jobs_left == 0)
        return self.observe(), reward, done

    def render(self):
        # time.sleep(0.01)
        self.update()
