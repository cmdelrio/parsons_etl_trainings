# This script pushes new mobilize contacts to Action Network and applies a tag

# --------------------------------------------------------------------------------------------------
# Import Necessary Packages
# --------------------------------------------------------------------------------------------------
import logging
from parsons import Table, Redshift, ActionNetwork
from datetime import datetime

# --------------------------------------------------------------------------------------------------
# Instantiate Classes/Connect to Redshift and ActionNetwork
# --------------------------------------------------------------------------------------------------
# See parsons documentation for how environmental variable should be stored
my_rs_warehouse = Redshift()
my_actionnetwork_group = ActionNetwork()

# Does anyone want to briefly explain logging and how it might be helpful here? This is different
# Than a log table.
# You don't need to uderstand this to use it!
logger = logging.getLogger(__name__)
_handler = logging.StreamHandler()
_formatter = logging.Formatter('%(levelname)s %(message)s')
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.setLevel('INFO')


# --------------------------------------------------------------------------------------------------
# Create a list that we'll turn into our log table
# --------------------------------------------------------------------------------------------------
# This list will be converted into a parsons table and stored in the data warehouse so that
# we have a record of which Mobilize records were synced successfully to Action Network and which
# were not
loglist=[]

# --------------------------------------------------------------------------------------------------
# Get mobilize records we need to sync from data warehouse
# --------------------------------------------------------------------------------------------------
sql_query = 'select * from mobilize_schema.mobilize_users_to_sync limit 5;'
new_mobilize_users = my_rs_warehouse.query(sql_query)

# Logging can help you understand what stage the script is in and debug the script if it fails
logger.info(f'''There are {new_mobilize_users.num_rows} new mobilize users that need to be synced to
Action Network.''')

if new_mobilize_users.num_rows > 0:
    logger.info('Starting the sync now.')

# --------------------------------------------------------------------------------------------------
# Sync records to Action Network
# --------------------------------------------------------------------------------------------------
for mobilize_user in new_mobilize_users:
    # Using try and except allows the sync to keep running even if there are errors with
    # individual records. We're going to try and load each new mobilize user into Action Network,
    # and if we succeed, we'll add a success record to the log table. If we hit an error,
    # instead of the script breaking, we'll just add an error record to the log table.
    try:
        # Looping through a parsons table returns each row as a dictionary
        # We can easily plug dictionary values into the actionnetwork.add_person() method
        actionnetwork_user = my_actionnetwork_group.add_person(
            email_address=mobilize_user['email_address'],
            given_name=mobilize_user['given_name'],
            family_name=mobilize_user['family_name'],
            mobile_number=mobilize_user['phone_number'],
            tag='Mobilize Event Attendee',
            # addresses are fed in as a list of dictionarys where each item in the list is a
            # different address and each item in the ditionary is a different component of the
            # address (e.g. state, city, street number, postal code). All we have is postal code
            # This is a good chance to check the docs! https://move-coop.github.io/parsons/html/stable/action_network.html
            postal_addresses=[
                {
                    'postal_code': mobilize_user['postal_code']
                }
            ]
        )

        # Get Action Network ID
        # ID is stored in this item called identifiers
        identifiers = actionnetwork_user['identifiers']
        # This cuts out all of the text except for the exact identifier
        actionnetworkid = [entry_id.split(':')[1]
                           for entry_id in identifiers if 'action_network:' in entry_id][0]

        # Create a record of our great success
        log_record = {
            'mobilizeid': mobilize_user['mobilizeid'],
            'actionnetworkid': actionnetworkid,
            'synced': True,
            'errors': None,
            'date': str(datetime.now())
        }

        # Add the record of our success to the history books
        loglist.append(log_record)

    # This is not best practice. It's better for your except statement to be more specific. Example:
    # except APIError as e:
    # If there was an issue with the data for a specific row, it's okay to skip it and log it
    # If there's an error with our code, we want to catch it and pause the script.
    # That's why this bare exception isn't ideal.
    except Exception as e:
        logger.info(f'''Error for mobilize user {mobilize_user['mobilizeid']}.
        Error: {str(e)}''')

        # Create a record of our failures
        log_record = {
            'mobilizeid': mobilize_user['mobilizeid'],
            'actionnetworkid': None,
            'synced': False,
            'errors': str(e)[:999],
            'date': str(datetime.now())
        }

        # Add the record of our greatest failures to the history books
        loglist.append(log_record)

# Now we're going to store our log table in our Redshift data warehouse
if new_mobilize_users.num_rows > 0:
    logtable = Table(loglist)
    errors_count = logtable.select_rows("{synced} is False").num_rows
    success_count = logtable.select_rows("{synced} is True").num_rows

    logger.info(f'''Successfully synced {success_count} mobilize users 
and failed to sync {errors_count}''')

    my_rs_warehouse.copy(tbl=logtable, table_name='mobilize_schema.mobilize_to_actionnetwork_log',
                         if_exists='append', alter_table=True)
