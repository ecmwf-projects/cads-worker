from unittest.mock import patch

import pytest
from sqlalchemy.orm import Session

from cads_worker import worker


def mock_session_maker():
    return Session()


def test_ensure_session_decorator():
    # Test case 1: when session is None
    @worker.ensure_session
    def sample_function(self, session=None):
        assert isinstance(session, Session)
        return session

    with patch(
        "cads_worker.worker.create_session_maker", return_value=mock_session_maker
    ):
        result = sample_function(self=None)
        assert isinstance(result, Session)

    # Test case 2: when session is provided
    mock_session = Session()
    result = sample_function(self=None, session=mock_session)
    assert result is mock_session

    # Clean up
    mock_session.close()


def test_ensure_session_decorator_nested():
    # Test nested function calls with session
    @worker.ensure_session
    def outer_function(self, session=None):
        @worker.ensure_session
        def inner_function(self, session=None):
            return session

        return inner_function(self=None, session=session)

    with patch(
        "cads_worker.worker.create_session_maker", return_value=mock_session_maker
    ):
        result = outer_function(self=None)
    assert isinstance(result, Session)
    result.close()


def test_ensure_session_decorator_error():
    # Test error handling
    @worker.ensure_session
    def failing_function(self, session=None):
        raise ValueError("Test error")

    with pytest.raises(ValueError):
        failing_function(self=None)


call_count = 1


def test_ensure_session_retry():
    # Test retries

    context = worker.Context()

    @worker.ensure_session
    def failing_function(self, session=None):
        print("failing function called")
        raise worker.cads_broker.database.sa.exc.OperationalError(
            "Simulated DB error", None, None
        )

    with patch(
        "cads_worker.worker.create_session_maker", return_value=mock_session_maker
    ):
        with pytest.raises(worker.cads_broker.database.sa.exc.OperationalError):
            # This should raise after max retries
            failing_function(self=context)

    global call_count

    @worker.ensure_session
    def successful_function(self, session=None):
        global call_count
        if call_count < 3:
            print("failing function called - retries:", call_count)
            call_count += 1
            raise worker.cads_broker.database.sa.exc.OperationalError(
                "Simulated DB error", None, None
            )
        else:
            print("successful function called - retries:", call_count)
            return session

    with patch(
        "cads_worker.worker.create_session_maker", return_value=mock_session_maker
    ):
        result = successful_function(self=context)
    assert isinstance(result, Session)
