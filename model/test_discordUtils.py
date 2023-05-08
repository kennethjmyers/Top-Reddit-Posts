import pytest
import discordUtils as du
import sys
import os
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(THIS_DIR, '../'))
import configUtils as cu
import responses


@pytest.fixture(scope='module')
def cfg():
  cfg_file = cu.findConfig()
  cfg = cu.parseConfig(cfg_file)
  return cfg


def test_makeHeader(cfg):
  token=cfg['BOTTOKEN']
  assert du.makeHeader(token) == {"Authorization": f"Bot {token}"}


def test_createDM(cfg):
  sampleJson = {
    'id': '0000000000',
    'type': 1,
    'last_message_id': '1111111111',
    'flags': 0,
    'recipients': [
      {
        'id': '2222222222',
        'username':
          'sample_user',
        'global_name': None,
        'display_name': None,
        'avatar': 'some_string_id',
        'discriminator': '0000',
        'public_flags': 0,
        'avatar_decoration': None
      }
    ]
  }
  headers = du.makeHeader(cfg['BOTTOKEN'])
  # https://github.com/getsentry/responses/blob/master/README.rst#responses-as-a-context-manager
  with responses.RequestsMock() as rsps:
    rsps.add(
      responses.POST,
      'https://discord.com/api/v10/users/@me/channels',
      headers=headers,
      json=sampleJson,
      status=200
    )
    dm = du.createDM(cfg['BOTTOKEN'], cfg['MYSNOWFLAKEID'])
  assert dm == sampleJson


@pytest.fixture(scope='module')
def sampleDiscordMessageJson():
  return {
    'id': '0000000000',
    'type': 0,
    'content': 'this is a test post\n testing again',
    'channel_id': '1111111111',
    'author': {
      'id': '2222222222',
      'username': 'RedditBot',
      'global_name': None,
      'display_name': None,
      'avatar': 'some_string',
      'discriminator': '1708',
      'public_flags': 0,
      'bot': True,
      'avatar_decoration': None
    },
    'attachments': [],
    'embeds': [],
    'mentions': [],
    'mention_roles': [],
    'pinned': False,
    'mention_everyone': False,
    'tts': False,
    'timestamp': '2023-05-08T17:35:52.536000+00:00',
    'edited_timestamp': None,
    'flags': 0,
    'components': [],
    'referenced_message': None
  }


def test_createMessage(cfg, sampleDiscordMessageJson):
  headers = du.makeHeader(cfg['BOTTOKEN'])
  with responses.RequestsMock() as rsps:
    for channelId in cfg['CHANNELSNOWFLAKEID']:
      rsps.add(
        responses.POST,
        f"https://discord.com/api/v10/channels/{channelId}/messages",
        headers=headers,
        json=sampleDiscordMessageJson,
        status=200
      )
      res = du.createMessage(cfg['BOTTOKEN'], channelId, 'test')
  assert res.json() == sampleDiscordMessageJson


def test_discordMessageHandler(cfg, sampleDiscordMessageJson):
  headers = du.makeHeader(cfg['BOTTOKEN'])
  with responses.RequestsMock() as rsps:
    for channelId in cfg['CHANNELSNOWFLAKEID']:
      rsps.add(
        responses.POST,
        f"https://discord.com/api/v10/channels/{channelId}/messages",
        headers=headers,
        json=sampleDiscordMessageJson,
        status=200
      )
      du.discordMessageHandler(cfg['BOTTOKEN'], channelId, 'test')


@pytest.mark.xfail(reason="status code != 200")
def test_discordMessageHandler404(cfg, sampleDiscordMessageJson):
  headers = du.makeHeader(cfg['BOTTOKEN'])
  with responses.RequestsMock() as rsps:
    for channelId in cfg['CHANNELSNOWFLAKEID']:
      rsps.add(
        responses.POST,
        f"https://discord.com/api/v10/channels/{channelId}/messages",
        headers=headers,
        json=sampleDiscordMessageJson,
        status=404
      )
      du.discordMessageHandler(cfg['BOTTOKEN'], channelId, 'test')
