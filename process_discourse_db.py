"""
<Program>
  process_discourse_db.py

<Purpose>

  Provides functionality to:

   - harvest post and topic data (and some user data) from a Discourse forum

   - save such data to JSON files

   - generate topic digests

   - send topic digests via email using oAuth (e.g. to a Google Groups forum)



  Running this module directly results in the following:

   - Saves data from the Discourse Database into some JSON files:
     topics.json, posts.json, users.json, processed_post_data.json

   - If given a topic ID as a command-line argument, also emails that topic's
     digest in to the forum.

   - If given the command-line argument 'all', emails all topic digests
     in to the forum.



  Prerequisites for this module's full operation:
   - pip install google-api-python-client
   - pip install psycopg2
   - Create a Gmail API credential for a new project
     https://console.developers.google.com/start/api?id=gmail
     and download the client_id.json file. Renamed it to client_secret.json and
     put it in the working directory from which this script is to be run.
   - Run only using Python2.


"""
import psycopg2
import json
import datetime
import gmailer
import time
import os.path

# Column enumeration in the Topics, Posts, and Users tables in the
# Discourse database.
# Ensure that the columns are correct for your Discourse version and
# configuration.

P_PID = 0     # index of post ID column in posts table
P_UID = 1     # index of user ID column in posts table
P_TID = 2     # index of topic ID column in posts table
P_CREATED = 6 # index of created date column in posts table
P_UPDATED = 7 # index of updated date column in posts table
P_RAW = 4     # index of plaintext post contents column in posts table
P_COOKED = 5  # index of "cooked" html post contents column in posts table
P_IMGURL = 50 # index of attached image URL column in posts table

T_TID = 0   # index of topic ID column in topics table
T_UID = 7   # index of user ID column in topics table
T_TITLE = 1 # index of title column in topics table
T_CREATED = 3 # index of created date column in topics table

U_UID = 0   # index of user ID column in users table
U_UNAME = 1 # index of username column in users table
U_NAME = 2  # index of full name column in users table
U_EMAIL = 5 # index of email address column in users table

BACKUP_MAILER = None # Replace with emailing acount's email address.
FORUM_ADDRESS = None # replace with Google Group's email address.

DBNAME = None # replace with the name of the Discourse database

SLEEP_DURATION_BETWEEN_MAILINGS = 0.2
SLEEP_DURATION_BETWEEN_TOPIC_ROUNDS = 10

PATH_TO_UPLOADS_DIR = 'uploads/' # sitting in the working directory
INTERNAL_UPLOAD_PATH_PREFIX = '<forum_location>/uploads/'
INTERNAL_UPLOAD_PATH_PREFIX = '/forum/uploads/'


def serialize_datetime(obj):
  """
  Given a datetime object, returns a string representing it. Given any other
  object, simply returns that object. This is of use as a default serializer
  for content containing all JSON-serializable types except for datetimes.
  It can be passed to json.dump(..., default=serialize_datetime, ...).
  """
  if isinstance(obj, datetime.datetime):
    return obj.isoformat()
  else:
    return obj





def harvest_from_psql_db():

  # The following line may need to be edited to handle credentials, depending
  # on local access to the PostgreSQL database.
  conn = psycopg2.connect("dbname='" + DBNAME + "' host='localhost'")

  cur = conn.cursor()

  cur.execute(
      "SELECT * FROM topics "
      "WHERE topics.archetype = 'regular' "
      "AND topics.user_id != -1 "
      "AND topics.visible IS TRUE "
      "ORDER BY topics.id")

  topics = cur.fetchall()

  cur.execute(
      "SELECT posts.* FROM posts "
      "JOIN topics ON posts.topic_id = topics.id "
      "WHERE topics.archetype = 'regular' "
      "AND posts.user_deleted IS FALSE "
      "AND posts.post_type = 1 "
      "AND topics.user_id != -1 "
      "AND topics.visible IS TRUE "
      "ORDER BY posts.topic_id, posts.post_number")

  posts = cur.fetchall()

  print 'Retrieved ' + str(len(topics)) + ' topics ' \
      'containing ' + str(len(posts)) + ' posts.'

  cur.execute(
      "SELECT id, username, name, approved, blocked, email FROM users")

  users = cur.fetchall()

  print 'Retrieved ' + str(len(users)) + ' users.'


  return (topics, posts, users)





def print_to_json(topics, posts, users):
  # Now save the collected data as JSON.
  # We have to adjust datetime objects to make them serializable.
  with open('topics.json', 'w') as fobj:
    json.dump(topics, fobj, default=serialize_datetime, indent=2)

  with open('posts.json', 'w') as fobj:
    json.dump(posts, fobj, default=serialize_datetime, indent=2)

  with open('users.json', 'w') as fobj:
    json.dump(users, fobj, default=serialize_datetime, indent=2)





def generate_single_dict(topics, posts, users):
  """
  Example output:
  d = {
      1: {   # a topic ID and the associated topic
        'author': 'alice (Alice Bob (alice@bob.not)',
        'title': 'Look, a forum!',
        'created': '2017-01-01T01:00:00.000000',
        'posts': [
            {'author': 'alice (Alice Bob alice@bob.not)',
            'created': '2017-01-01T01:00:00.000000',
            'updated': '2017-01-01T01:00:00.000000',
            'image_url': None,
            'raw': 'Lorem ipsum... \n'
                   '\n'
                   'dolor sit amet...',
            'cooked': '<p>Lorem ipsum...</p>\n'
                      '\n'
                      '<p>dolor sit amet...</p>'},

            {'author': 'clarice/Clarice Starling(clarice@unfortunate.not)',
            'created': '2017-01-01T02:00:00.000000',
            'updated': '2017-01-01T02:00:00.000000',
            'image_url': None,
            'raw': 'Donec ante dolor.',
            'cooked': '<p>Donec ante dolor.</p>'},

            ... # more posts in the topic
        ]
      },
      ... # more topics
  }
  """
  d = {}

  usernames = {}
  names = {}
  emails = {}

  for user in users:
    uid = user[U_UID]

    usernames[uid] = user[U_UNAME]
    names[uid] = user[U_NAME]
    emails[uid] = user[U_EMAIL]


  for topic in topics:
    tid = topic[T_TID]
    uid = topic[T_UID]

    d[tid] = {
        'author': usernames[uid] + ' (' + names[uid] + ' ' + emails[uid] + ')',
        'title': topic[T_TITLE],
        'created': topic[T_CREATED].isoformat(),
        'posts': []}


  for post in posts:
    tid = post[P_TID]
    uid = post[P_UID]

    if uid is not None:
      author = usernames[uid] + ' (' + names[uid] + ' ' + emails[uid] + ')'
    else:
      author = 'UNKNOWN USER'

    d[tid]['posts'].append({
        'author': author,
        'created': post[P_CREATED].isoformat(),
        'updated': post[P_UPDATED].isoformat(),
        'raw': post[P_RAW],
        'cooked': post[P_COOKED],
        'image_url': post[P_IMGURL]})


  return d






def __OLD_construct_topic_digest(tid, d):
  """
  Given the dictionary of topics and posts as generated by
  generate_single_dict() and the ID of a topic (tid), returns a string
  topic digest including all the posts in that topic in order, with some
  formatting and basic info on the posts.

  Returns two strings, the first being compatible with a plain email and the
  second being compatible with an HTML email.
  """

  topic = d[tid]

  digest = 'This topic has been transfered from the Discourse forum to '
  digest += 'this Google Group automatically.\nThe original post and all '
  digest += 'replies are included. \n\n'
  digest += 'Topic: ' + topic['title'] + '\n'
  digest += 'Created By: ' + topic['author'] + '\n'
  digest += 'Topic Date: ' + topic['created'] + '\n\n'

  assert(len(topic['posts'])), 'Topic containing no posts??'

  for i in range(len(topic['posts'])):

    post = topic['posts'][i]

    digest += '------------------------------------------------------------'
    digest += '------------------\n'

    digest += '--Post ' + str(i + 1) + ' of Topic "'

    if len(topic['title']) > 39: # 39 character max from topic title
      digest += topic['title'][:36] + '...'
    else:
      digest += topic['title']

    digest += '"\n'

    digest += 'Post Author: ' + post['author'] + '\n'
    digest += 'Created: ' + post['created'] + '\n'

    if post['created'] != post['updated']:
      digest += 'Updated: ' + post['updated'] + '\n'

    if post['image_url'] is not None:
      digest += 'Attached Image URL: ' + post['image_url'] + '\n'

    digest += '\n'


    digest_cooked = digest.replace('\n', '<br />')

    digest += post['raw'] + '\n\n\n'
    digest_cooked += post['cooked'] + '<br /><br /><br />'


  return digest, digest_cooked






def __OLD_add_all_topic_digests(d):
  """
  OLD FUNCTION - entire topic as single post

  Given the dictionary of topics and posts as generated by
  generate_sample_dict(), adds plaintext and html topic digests to that
  dictionary. (see construct_topic_digest()).
  """
  for tid in d:
    digest = __OLD_construct_topic_digest(tid, d)
    d[tid]['digest_plain'] = digest[0]
    d[tid]['digest_cooked'] = digest[1]







def __OLD_mail_in_topic_digest(d, tid):
  """
  OLD FUNCTION - entire topic as single post

  The gmailer module uses pre-configured authentication via oauth.
  """

  if tid not in d:
    raise Exception('Unknown tid ' + str(tid))

  elif 'digest_plain' not in d[tid] or 'digest_cooked' not in d[tid]:
    raise Exception('No digest key for tid ' + str(tid))

  elif not d[tid]['digest_plain'] or not d[tid]['digest_cooked']:
    raise Exception('Empty digest for tid ' + str(tid))

  print 'Sending in digests for topic ID ' + str(tid) + '...'

  gmailer.SendMessage(
      BACKUP_MAILER,
      FORUM_ADDRESS,
      d[tid]['title'],
      d[tid]['digest_cooked'], # html (cooked)
      d[tid]['digest_plain'])  # raw





def mail_topic_as_posts(d, tid):
  """
  Alternate method of operation:
  Post posts individually.
  """

  # Send first post and save threadid.

  print 'Mailing topic ' + str(tid) + ' as individual posts.'

  # Send first post.
  (text_plain, text_html) = construct_post_email_contents(d, tid, 0)
  result = gmailer.SendMessage(
      BACKUP_MAILER,
      FORUM_ADDRESS,
      d[tid]['title'],
      text_html,
      text_plain)

  # SendMessage returns something like this:
  # {'labelIds': ['SENT'],
  #  'id': '1593541c29112345',
  #  'threadId': '1593501de4512367'}

  if not isinstance(result, dict) or 'labelIds' not in result or \
      'SENT' not in result['labelIds']:
    raise Exception('Unable to send initial message for topic ' + str(tid) +
        '; result is: ' + repr(result))

  previous_message_id = result['id'] # not currently used, but could avoid races? -- no. The reply-to reference should be set using information received by the recipient, I think.... Or, actually, by the mail server. We don't have that info.

  thread_id = result['threadId']

  for i in range(1, len(d[tid]['posts'])):
    (text_plain, text_html) = construct_post_email_contents(d, tid, i)

    image_url = process_image_url(d, tid, i)

    print 'Mailing ' + str(i) + 'th post.'
    result = gmailer.SendMessage(
      BACKUP_MAILER,
      FORUM_ADDRESS,
      d[tid]['title'],
      text_html,
      text_plain,
      image_url,  # attachment
      thread_id,
      previous_message_id)

    if not isinstance(result, dict) or 'labelIds' not in result or \
        'SENT' not in result['labelIds']:
      raise Exception('Unable to send post ' + str(i) + ' in topic ' +
          str(tid) + '; previous message id was: ' + previous_message_id)

    previous_message_id = result['id'] # not currently used, but could avoid races? -- no. The reply-to reference should be set using information received by the recipient, I think.... Or, actually, by the mail server. We don't have that info.

    time.sleep(SLEEP_DURATION_BETWEEN_MAILINGS)






def mail_topics_as_posts_bf(d, tids=None):
  """
  Mail all (or select) topics by individual post, breadth-first in order to
  cut down on messages that are not in order.
  (the reply-to component of a mail message requires the value provided to
  the recipient of the email message, and so cannot be used to fix the sequence
  of a set of email sent sequentially by the same sender. Gmail assigns the
  references (reply-to) elements when it has processed each new message, and
  the duration of delay in between messages to prevent race conditions leading
  to posts out of order is actually quite long (certainly longer than 1s,
  probably longer than 10s)).

  On the other hand, if we send the first message of every topic first, then
  the second message of every topic, then the third message of every topic,
  etc., then the only rate limitation we have is that required to not anger
  Google for sending messages too quickly. 0.2s should suffice there.

  """

  if tids is None:
    tids = d.keys()

  most_posts_in_one_topic = 0
  previous_message_id_by_tid = {} # probably not useful

  for tid in tids:

    if len(d[tid]['posts']) > most_posts_in_one_topic:
      most_posts_in_one_topic = len(d[tid]['posts'])

    if tid not in d:
      raise Exception('Topic ID ' + str(tid) + ' is not known.')

    d[tid]['thread_id'] = None
    previous_message_id_by_tid[tid] = None # probably not useful


  for post_number in range(0, most_posts_in_one_topic):
    print('Sending post # ' + str(post_number) + ' in all topics.')
    for tid in tids:

      if post_number >= len(d[tid]['posts']):
        continue

      (text, text_html) = construct_post_email_contents(d, tid, post_number)

      image_url = process_image_url(d, tid, post_number)

      print 'Mailing post ' + str(post_number) + ' in topic ' + str(tid)
      result = gmailer.SendMessage(
        BACKUP_MAILER,
        FORUM_ADDRESS,
        d[tid]['title'],
        text_html,
        text,
        image_url,  # attachment
        d[tid]['thread_id'],
        previous_message_id_by_tid[tid])

      # Stop if a message fails to send.
      if not isinstance(result, dict) or 'labelIds' not in result or \
          'SENT' not in result['labelIds']:
        raise Exception('Unable to send post ' + str(i) + ' in topic ' +
            str(tid) + '; previous message id was: ' + previous_message_id)

      time.sleep(SLEEP_DURATION_BETWEEN_MAILINGS)

      # Save thread ID so we can send the next post in the same topic to the
      # same thread.
      d[tid]['thread_id'] = result['threadId']
      previous_message_id_by_tid[tid] = result['id'] # not currently used, but could avoid races? -- no. The reply-to reference should be set using information received by the recipient, I think.... Or, actually, by the mail server. We don't have that info.

    time.sleep(SLEEP_DURATION_BETWEEN_TOPIC_ROUNDS)





def construct_post_email_contents(d, tid, post_number):
  """
  Returns 2-tuple containing:
   - plaintext message
   - html message
  """

  if tid not in d:
    raise Exception('Topic ' + str(tid) + ' is not known.')

  elif 'posts' not in d[tid]:
    raise Exception('Topic entry ' + str(tid) + ' lacks expected structure.')

  elif len(d[tid]['posts']) < post_number:
    raise Exception('Topic ' + str(tid) + ' does not have a ' +
        str(post_number) + 'th reply.')

  topic_title = d[tid]['title']
  post = d[tid]['posts'][post_number]


  text = ''

  if post_number == 0:
    text += 'Topic posted by Discourse-to-Google-Groups forum transfer.\n'

  text += 'Post ' + str(post_number + 1) + ' of Topic "'

  if len(topic_title) > 39: # 39 character max from topic title
    text += topic_title[:36] + '...'
  else:
    text += topic_title

  text += '"\n'

  text += 'Post Created: ' + post['created'] + '\n'

  if post['created'] != post['updated']:
    text += 'Post Updated: ' + post['updated'] + '\n'

  if post['image_url'] is not None:
    text += 'Attached Image URL: ' + post['image_url'] + '\n'

  text += 'Post Author ' + post['author'] + ' wrote:\n\n'

  text_html = text.replace('\n', '<br />')

  text += post['raw'] + '\n\n\n'
  text_html += post['cooked'] + '<br /><br /><br />'


  return text, text_html






def process_image_url(d, tid, post_number):

  """
  Not secure. (ex-filtration)

  Returns the image_url of any attachment to a post, if the attachment is
  available locally in the filesystem. Else returns None.
  """

  image_url = d[tid]['posts'][post_number]['image_url']

  if image_url is None:
    return None

  elif image_url.startswith('http://') or image_url.startswith('https://'):
    print 'Skipping attachment that was added as an http(s) link, ' \
        'topic ' + str(tid) + ', post # ' + str(post_number) + '; ' \
        'url: ' + image_url
    return None

  elif image_url.startswith(INTERNAL_UPLOAD_PATH_PREFIX):
    # Cut out path prefix for uploads directory if it's there and replace
    # with location of uploads directory.
    image_url = PATH_TO_UPLOADS_DIR + image_url[len(
        INTERNAL_UPLOAD_PATH_PREFIX):]

  if not os.path.exists(image_url):
    print 'Skipping attachment that cannot be found locally. ' \
        'topic ' + str(tid) + ', post # ' + str(post_number) + '; ' \
        'url: ' + image_url
    return None

  else:
    return image_url





def main():
  """
  See top of module for docstring.
  """

  (topics, posts, users) = harvest_from_psql_db()

  print_to_json(topics, posts, users)

  d = generate_single_dict(topics, posts, users)

  # __OLD_add_all_topic_digests(d)

  with open('processed_post_data.json', 'w') as fobj:
    json.dump(d, fobj, indent=2)


  if len(sys.argv) == 2 and sys.argv[1] == 'all':
    for tid in d:
      # __OLD_mail_in_topic_digest(d, tid)
      mail_topic_as_posts(d, tid)
      time.sleep(SLEEP_DURATION_BETWEEN_MAILINGS)
    return

  elif len(sys.argv) == 2:
    tid_to_email = int(sys.argv[1])

    if tid_to_email not in d:
      raise Exception('Unknown topic ID ' + str(tid))

    else: # redundant control (if clause raised exception)
      mail_in_topic_digest(d, tid_to_email)





if __name__ == '__main__':
  main()
