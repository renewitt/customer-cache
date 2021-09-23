""" Initalise PI. """
import yaml

import mpi.service
import mpi.rabbit

CONFIG_PATH = "config.yml"

def load_config(config_file):
    """
    This method will be replaced by mconfig3 functionality.

    TODO: Use argparse with this as well so we can pass params on the command line, though mconfig
    will do this for us also - may just need to do it for containers if we get that far during WFH.
    Otherwise, we will typically always be loading from config file so this implementation is
    sufficient for a POC.
    """
    with open(config_file) as f:
        conf = yaml.safe_load(f)
    return conf

def main():
    """ Initalise PI. """
    conf = load_config(CONFIG_PATH)

    # initialise rabbitmq client
    rabbit = mpi.rabbit.Rabbit(
        conf['rabbitmq_host'],
        conf['rabbitmq_user'],
        conf['rabbitmq_password'],
        conf['consumer_bindings'],
    )

    pi = mpi.service.Pi(
        rabbit,
        conf['refresh_time'],
        conf['manifest_size'],
        conf['cooldown_time'],
        conf['active_time'],
        )
    pi.run()
