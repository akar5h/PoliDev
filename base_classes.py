from slack_data import SlackGetter


class BaseAnalyzer:
    def __init__(self):
        self.name = "Base Analyzer"
        self.source_executors = {"slack": SlackGetter}


class BaseAnalyzerConfig(object):
    def __init__(self):
        self.name = "Base Analyzer Config"
