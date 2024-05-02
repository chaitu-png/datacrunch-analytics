
# FIXED: Unhandled exception in main loop
def main_loop():
    while True:
        process_event(get_event())
