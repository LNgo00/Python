from telethon.sync import TelegramClient
from telethon.tl.types import ChannelParticipantsContacts
from telethon.tl.types import ChannelParticipantsAdmins
from telethon.tl.types import InputMessagesFilterContacts
from telethon import types
from telethon import errors
import os
import sys
import mysql.connector
import datetime
import time
import logging

logging.basicConfig(format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s', level=logging.DEBUG)

print((sys.argv))
if len(sys.argv) < 2:
	raise Exception('Please call telegram.py with telephone number as arg. Note: Bot token cannot be used due to bot access restrictions.')

telNumber = sys.argv[1]
logging.info('Starting script using telephone number: ' + telNumber)

# my.telegram.org
api_id = os.getenv('TELEGRAM_ID')
api_hash = os.getenv('TELEGRAM_HASH')

# sql config
logging.info('Connecting to MySQL...')
db = mysql.connector.connect(
	host = os.getenv('MYSQL_SERVER'),
	database = os.getenv('MYSQL_DATABASE'),
	user = os.getenv('MYSQL_USER'),
	passwd = os.getenv('MYSQL_PASSWORD')
)

startTimestamp = datetime.datetime.now()

cursor = db.cursor()
cursor.execute("""
	SELECT telegram_handle, updated_at FROM stats_telegram 
	WHERE updated_at < subdate(curdate(), interval 2 day) OR updated_at IS NULL
	ORDER BY telegram_users DESC, updated_at ASC
""")
	
result = cursor.fetchall()
logging.info('Fetched %s project telegram handles' % (len(result)))

rows = []

for data in result:
	if not data[1] == None:
		rows.append(data)

logging.info('Using %s of project telegram handles' % (len(rows)))
		
logging.info('Init TelegramClient (request_retries=10, flood_sleep_threshold=120) and log using telephone number.')
client = TelegramClient('dev', api_id, api_hash, request_retries=10, flood_sleep_threshold=120).start(phone=telNumber)
with client:

	# Interate over chats
	index = 0
	for row in rows:
		index = index + 1
		chat = row[0]
		date = row[1]
		users = 0
		users_active = 0
		users_verified = 0
		users_posted = {}
		admins = []
		admin_count = 0
		messages = 0

		if chat is None:
			continue
		print("> Checking " + chat)
		logging.info('> Checking ' + chat)

		try:

			# Filter out non-megagroups
			chat_info = client.get_entity(chat)
			if not hasattr(chat_info, 'megagroup') or (hasattr(chat_info, 'megagroup') and not chat_info.megagroup == True):
				print(">> Not megagroup, skipping with 0.5s sleep... ")
				time.sleep(.5)
				continue

			# Get User (total, active and verified) count
			contacts = client.get_participants(chat, limit=None)
			print(">> Total participants found: " + str(len(contacts)))
			for contact in contacts:
				# Filter out bot messages
				if contact.bot:
					continue

				users = users + 1

				if isinstance(contact.status, types.UserStatusOnline) or isinstance(contact.status, types.UserStatusRecently) or isinstance(contact.status, types.UserStatusLastWeek):
					users_active = users_active + 1

				if contact.verified:
					users_verified = users_verified + 1
			logging.info('>> %s users ' % (users))

			# Get Admins ids and count
			for admin in client.get_participants(chat, filter=ChannelParticipantsAdmins, limit=None):
				admins.insert(1, admin.id)
			admin_count = len(admins)
			logging.info('>> %s admins ' % (admin_count))

			# Get contact Message count and Users posted since updated_at date
			chatMessages = client.get_messages(chat, reverse=True, offset_date=date, limit=None)
			print(">> Total chat messages found: " + str(len(chatMessages)))
			for message in chatMessages:  # y, m, d, h, m,s

					# Filter out bot messages
					if not message.via_bot_id  == None:
						continue
					
					# Add non-Admin Messages and add to Users who posted to unique ids dict
					if not message.from_id in admins:
						messages = messages + 1
						users_posted[message.from_id] = message.from_id
			logging.info('>> %s messages ' % (messages))

		
		except errors.FloodWaitError as e:
			print('Flood Wait', e.seconds, 'seconds')
			logging.error('Flood Wait %s seconds' % (e.seconds))
			time.sleep(e.seconds)

		except (errors.RPCError, ValueError) as e:
			print("- Error, skipping... " + str(e))
			logging.error('- Error, skipping... '+ str(e))

		except Exception as e:
			print(e)
			logging.error('- Error - '+ str(e))

		sql = """UPDATE stats_telegram SET telegram_messages_week = %s, telegram_users = %s, telegram_admins = %s, 
			telegram_users_active = %s, telegram_users_verified = %s, telegram_users_posted = %s WHERE telegram_handle = %s"""
		val = (messages, users, admin_count, users_active, users_verified, len(users_posted), row[0])
		cursor.execute(sql, val)
		logging.info('- Commiting updates to database')
		db.commit()

		logging.info('< Total users (%s), admins (%s) & messages (%s)' % (users, admin_count, messages))
		print('-------------------------------------------------')
		print('--> ' + str(index) + ' of ' + str(len(result)) + ' project telegram handles <--')
		print('-------------------------------------------------')

		cooldownTime = 2
		logging.info('Cooling down for %s seconds...' % (cooldownTime))
		time.sleep(cooldownTime)  # cooldown

logging.info('Completed time: ' + str(datetime.datetime.now() - startTimestamp))
