# -*- coding: utf-8 -*-

""" Used to mock the integration between chorde and different frameworks/libraries
"""

def monkey_patch_tornado():
    """ Mock the tornado.conncurrent.is_future to mark the chorde 
    Futures as valid.

    This is required, because Tornado checks that the result is a
    valid future, so if this isn't monkey patched when using
    chorde Futures is going to raise an exception
    """
    from tornado import concurrent
    from .clients.async import Future
    original_futures = concurrent.FUTURES
    if isinstance(original_futures, tuple):
        # on tornado 4.X it could be a tuple or just one class
        new_futures = list(original_futures)
        new_futures.append(Future)
        new_futures = tuple(new_futures)
    else:
        new_futures = (original_futures, Future)

    concurrent.FUTURES = new_futures


