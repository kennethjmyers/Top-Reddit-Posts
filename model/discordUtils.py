import requests


def makeHeader(botToken: str):
  """
  Generates dictionary of the header to pass to requests. Header has bot token for authentication

  :param botToken: bot token
  :return: headers
  """
  headers = {"Authorization": f"Bot {botToken}"}
  return headers


def createDM(botToken: str, userSnowflakeId: str):
  """
  Opens a DM with someone or returns the DM if already exists
  https://discord.com/developers/docs/resources/user#create-dm

  :param botToken: bot token
  :param userSnowflakeId: snowflake ID of user to DM
  :return: json for DM
  """
  headers = makeHeader(botToken)
  dm = requests.post(
    url='https://discord.com/api/v10/users/@me/channels',
    headers=headers,
    json={"recipient_id": userSnowflakeId}
  ).json()
  return dm


def createMessage(botToken: str, channelId: str,  message: str):
  """
  Opens a DM with someone or returns the DM if already exists
  https://discord.com/developers/docs/resources/channel#create-message

  :param botToken: bot token
  :param channelId: a snowflake ID, this could be a DM id (ie dm['id']) or a channel ID
  :param message: Message text
  :return: response data
  """
  headers = makeHeader(botToken)
  response = requests.post(
    f"https://discord.com/api/v10/channels/{channelId}/messages",
    headers=headers,
    json={"content": message}
  )
  return response
