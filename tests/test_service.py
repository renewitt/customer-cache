""" Tests the MPI service class. """
import pytest
import mpi.service as svc

import tests.conftest as helpers

def test_message_callback(mocker):
    """ Test message callback. """
    message = mocker.Mock()
    message.application_headers = helpers.gen_headers()

    pi = mocker.Mock()
    message.delivery_info = {'routing_key': 'bad'}
    svc.Pi.message_callback(pi, message)
    assert pi.rabbit.reject_message.called
    assert not pi.on_start.called
    assert not pi.on_stop.called

    pi = mocker.Mock()
    message.delivery_info = {'routing_key': 'start'}
    message.application_headers = helpers.gen_headers()
    svc.Pi.message_callback(pi, message)
    assert pi.on_start.called
    assert not pi.on_stop.called
    assert not pi.rabbit.reject_message.called

    pi = mocker.Mock()
    message.delivery_info = {'routing_key': 'stop'}
    message.application_headers = helpers.gen_headers()
    svc.Pi.message_callback(pi, message)
    assert pi.on_stop.called
    assert not pi.on_start.called
    assert not pi.rabbit.reject_message.called

def test_callback_error(mocker):
    """
    Test that bad headers cause messages to be rejected and logged. Anything else causes
    the service to stop.
    """
    pi = mocker.Mock()
    message = mocker.Mock()
    message.delivery_info = {'routing_key': 'start'}
    message.application_headers = {'bad': 'headers'}
    mocker.patch('mpi.dbapi.create_cache')
    pi.rabbit.consume.return_value = svc.Pi.message_callback(pi, message)
    svc.Pi.run(pi)
    assert pi.rabbit.reject_message.called
    assert not pi.on_start.called
    assert not pi.on_stop.called

    # Any error that isn't because of the headers is actually raised
    pi = mocker.Mock()
    pi.on_start.side_effect = RuntimeError
    message = mocker.Mock()
    message.delivery_info = {'routing_key': 'start'}
    message.application_headers = helpers.gen_headers()
    with pytest.raises(RuntimeError):
        svc.Pi.message_callback(pi, message)
    assert not pi.on_stop.called

def test_send_to_cooldown(mocker):
    """ Test that if the cache is too big, both the cooldown and backfilling occurs. """
    pi = mocker.Mock()
    pi.conn = mocker.MagicMock()
    pi.manifest_size = 5

    mock_cooldown = mocker.patch('mpi.dbapi.update_to_cooldown')
    mock_free = mocker.patch('mpi.dbapi.update_free_cooldown')
    # return a list of 10 items on the first call to this mock, and 2 items the second call
    # this allows us to simulate a database call which retrieves all the records
    mock_select = mocker.patch('mpi.dbapi.select_manifest_records', side_effect=[
        ["a"]*10, ["a"]*2])

    svc.Pi._send_to_cooldown(pi)
    assert mock_select.call_count == 2
    assert mock_cooldown.called
    assert mock_free.called

def test_generate_manifest(mocker):
    """
    Test trimming the manifest when we have too many records after prune and cooldown.
    """
    pi = mocker.Mock()
    pi.conn = mocker.MagicMock()
    pi.manifest_size = 2
    mocker.patch('mpi.dbapi.select_manifest_records', return_value=["a"]*5)
    mock_update = mocker.patch('mpi.dbapi.update_to_tasked')

    manifest = svc.Pi.generate_manifest(pi)
    assert pi._prune_cache.called
    assert pi._send_to_cooldown.called
    assert mock_update.called
    assert len(manifest) == 2

def test_on_start(mocker):
    """ Test customers are either added or updated, not both. """
    pi = mocker.Mock()
    pi.conn = mocker.MagicMock()
    mocker.patch('mpi.dbapi.select_record', side_effect=[None, 1])

    mock_insert = mocker.patch('mpi.dbapi.insert_record')
    mock_update = mocker.patch('mpi.dbapi.update_active_time')
    svc.Pi.on_start(pi, '1111', '127.0.0.1', 'hogwarts', 'wizard', '111-111')
    assert mock_insert.called
    assert not mock_update.called

    mock_insert = mocker.patch('mpi.dbapi.insert_record')
    mock_update = mocker.patch('mpi.dbapi.update_active_time')
    svc.Pi.on_start(pi, '1111', '127.0.0.1', 'hogwarts', 'wizard', '111-111')
    assert mock_update.called
    assert not mock_insert.called