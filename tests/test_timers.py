""" Tests the timer functionality of mrabbit. """
import datetime

import mpi.rabbit as rabbit

def test_add_timer(mocker):
    """
    Test adding, and re-adding a timer to a rabbit instance.
    """
    r = mocker.Mock()
    r.timers = {}
    rabbit.Rabbit.add_timer(r, 'mytimer', 10, print)
    timer = r.timers['mytimer']
    assert timer.interval == datetime.timedelta(seconds=10)
    assert timer.method == print

    # adding the timer again with updated values causes it to be replaced
    my_method = mocker.Mock()
    rabbit.Rabbit.add_timer(r, 'mytimer', 20, my_method)
    assert len(r.timers) == 1
    timer = r.timers['mytimer']
    assert timer.interval == datetime.timedelta(seconds=20)
    assert timer.method == my_method

def test_check_timers(mocker):
    """ Test run the timers. """
    my_method = mocker.Mock()
    r = mocker.Mock()
    r.timers = {}
    rabbit.Rabbit.add_timer(r, 'mytimer', 0, my_method)
    rabbit.Rabbit.check_timers(r)
    assert my_method.called

def test_delete_timer(mocker):
    """ Testing handling deletion and cleanup of timers. """
    r = mocker.Mock()
    r.timers = {
        'mytimer': mocker.Mock(),
        'myothertimer': mocker.Mock()}
    rabbit.Rabbit.delete_timer(r, 'mytimer')
    rabbit.Rabbit.delete_timer(r, 'nonexistent')