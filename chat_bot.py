import asyncio

from telethon import TelegramClient, events
from telethon.tl.custom.button import Button

from get_data_from_db import is_json
from get_data_from_db import process_query


api_id = "api_id"
api_hash = "api_hash"
bot_token = "bot_token"
bot = TelegramClient("bot", api_id, api_hash).start(bot_token=bot_token)


async def main():
    async with bot:

        @bot.on(events.NewMessage)
        async def handler(event):
            if event.raw_text == "/start":
                sender = await event.get_sender()
                response = f"Hi [{sender.first_name}](tg://user?id={sender.id})!"
                await event.respond(response, buttons=Button.force_reply())
            else:
                is_query = loop.create_task(is_json(event.raw_text))
                await asyncio.wait([is_query])
                if is_query.result() == True:
                    result = await process_query(loop, event.raw_text)
                    try:
                        await event.respond(result)
                    except Exception as ex:
                        await event.respond(str(result.result()).replace("'", '"'))
                else:
                    await event.respond(
                        "Невалидный запос. Пример запроса:\n"
                        + '{"dt_from": "2022-09-01T00:00:00", "dt_upto": '
                        + '"2022-12-31T23:59:00", "group_type": "month"}'
                    )

        await bot.run_until_disconnected()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()
