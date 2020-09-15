import time
import logging
import sched
import threading
import os
from decimal import InvalidOperation, Decimal
from logging.config import dictConfig


import click
from tqdm import tqdm
import gspread
from flask import Flask, request, Response, url_for

from viberbot import Api
from viberbot.api.bot_configuration import BotConfiguration
from viberbot.api.messages.text_message import TextMessage
from viberbot.api.viber_requests import (
    ViberConversationStartedRequest,
    ViberFailedRequest,
    ViberMessageRequest,
    ViberSubscribedRequest,
    ViberUnsubscribedRequest,
)

from viberbot.api.messages import TextMessage, ContactMessage, PictureMessage, VideoMessage, KeyboardMessage


from exc import InvalidSheet, InvalidRecord
from storage import get_postgres_database
from dicts import REGIONS, PRODUCT_CATEGORIES, HEADERS
from keyboards import VIBER_MENU_KBD, VIBER_REGIONS_KBD, get_viber_categories_kbd, get_viber_subscribe_kbd
from utils import parse_amount, parse_int


app = Flask(__name__)
app.config.from_object("default_settings")
db = get_postgres_database(app)
procurements = db["procurements"]
dictConfig(app.config["LOGGING"])

viber = Api(
    BotConfiguration(
        name=app.config["BOT_NAME"],
        avatar="http://viber.com/avatar.jpg",
        auth_token=app.config["BOT_AUTH_TOKEN"],
    )
)


def get_product_stats(region, product_category):
    prices = []
    amounts = []

    for p in procurements.all(product_name=product_category, region=region):
        prices.append(Decimal(p["price"]))
        amounts.append(Decimal(p["total_amount"]))

    if prices:
        return {
            "count": len(prices),
            "total": sum(amounts),
            "max": max(prices),
            "min": min(prices),
            "avg": sum(prices) / len(prices),
        }
    else:
        return None


@app.cli.command("sync_spreadsheet")
def sync_spreadsheet():
    inserted_count = 0
    updated_count = 0
    invalid_count = 0
    useful_sheets = 0
    invalid_sheets = 0

    gc = gspread.service_account(os.path.join("keys", app.config["GDRIVE_KEY"]))
    sp = gc.open_by_key(app.config["GDRIVE_SPREADSHEET"])
    for sheet_num, sheet in enumerate(tqdm(sp.worksheets(), desc="Sheets")):
        try:
            for rec in tqdm(sheet.get_all_records(), desc=f"Records in sheet {sheet_num + 1}"):
                refined_rec = {}

                try:
                    for k, v in rec.items():
                        k = k.lower()
                        if k not in HEADERS:
                            app.logger.warning(f"Cannot parse header record {k}, aborting current sheet {sheet}")
                            raise InvalidSheet()

                        new_k = HEADERS[k]

                        if new_k == "product_name":
                            if v.lower() not in PRODUCT_CATEGORIES:
                                app.logger.warning(f"Cannot parse product_category {v}, skipping rec {rec}")
                                raise InvalidRecord()

                            refined_rec[new_k] = PRODUCT_CATEGORIES[v.lower()]
                        elif new_k == "region":
                            if v.lower() not in REGIONS:
                                app.logger.warning(f"Cannot parse region {v}, skipping rec {rec}")
                                raise InvalidRecord()

                            refined_rec[new_k] = REGIONS[v.lower()]
                        elif new_k == "product_details":
                            refined_rec[new_k] = v
                            refined_rec["product_hash"] = v.lower().strip()
                        elif new_k in ["price", "total_amount"]:
                            try:
                                refined_rec[new_k] = parse_amount(v)
                            except InvalidOperation:
                                app.logger.warning(f"Cannot parse price {v}, skipping rec {rec}")
                                raise InvalidRecord()
                        elif new_k in ["participants"]:
                            try:
                                refined_rec[new_k] = parse_int(v)
                            except ValueError:
                                app.logger.warning(f"Cannot parse price {v}, skipping rec {rec}")
                                raise InvalidRecord()
                        else:
                            refined_rec[new_k] = v

                    update = procurements.upsert(refined_rec, ["contract_id", "product_name", "product_hash"])

                    if update == True:
                        updated_count += 1
                    else:
                        inserted_count += 1
                except InvalidRecord:
                    invalid_count += 1
                    continue

            useful_sheets += 1
        except InvalidSheet as e:
            invalid_sheets += 1

    app.logger.info(f"Sheets processed: {useful_sheets}, sheets skipped: {invalid_sheets}")
    app.logger.info(
        f"Records added: {inserted_count}, records updated: {updated_count}, records skipped: {invalid_count}"
    )


@app.route("/export", methods=["GET"])
def export():
    return None


@app.route("/", methods=["POST"])
def incoming():
    app.logger.debug(f"received request. post data: {request.get_data()}")

    viber_request = viber.parse_request(request.get_data().decode("utf8"))

    if isinstance(viber_request, ViberMessageRequest):
        message = viber_request.message
        chunks = message.text.split(":")

        if not chunks:
            command = "start"
        else:
            command = chunks[0]

        if command == "start":
            viber.send_messages(
                viber_request.sender.id,
                TextMessage(
                    text="Для того щоб розпочати роботу оберіть внизу область по котрій ви хочете отримувати цінову інформацію",
                    keyboard=VIBER_REGIONS_KBD,
                ),
            )
        elif command == "help":
            viber.send_messages(
                viber_request.sender.id,
                TextMessage(
                    text="Бот дозволяє вам отримувати актуальну інформацію щодо цінових пропозицій на різні категорії товарів а також підписуватися на такі цінові пропозиції",
                    keyboard=VIBER_MENU_KBD,
                ),
            )
        elif command == "subscriptions":
            viber.send_messages(
                viber_request.sender.id,
                TextMessage(text="У вас поки що нема активних підписок", keyboard=VIBER_MENU_KBD),
            )
        elif command == "subscribe":
            viber.send_messages(
                viber_request.sender.id,
                TextMessage(
                    text=f"Дякую, ви успішно підписані на оновлення по категорії ”{chunks[2]}” в області ”{chunks[1]}”",
                    keyboard=VIBER_MENU_KBD,
                ),
            )
        elif command == "region":
            if len(chunks) > 1 and chunks[1] in REGIONS.values():
                viber.send_messages(
                    viber_request.sender.id,
                    TextMessage(text="Оберіть категорію товару", keyboard=get_viber_categories_kbd(chunks[1])),
                )
            else:
                viber.send_messages(
                    viber_request.sender.id,
                    TextMessage(text="Вибачте, не зрозумів, оберіть, будь ласка, область", keyboard=VIBER_REGIONS_KBD),
                )
        elif command == "product_category":
            if len(chunks) < 3 or chunks[1] not in REGIONS.values() or chunks[2] not in PRODUCT_CATEGORIES.values():
                viber.send_messages(
                    viber_request.sender.id,
                    TextMessage(text="Вибачте, не зрозумів, оберіть, будь ласка, область", keyboard=VIBER_REGIONS_KBD),
                )
            else:
                stats = get_product_stats(chunks[1], chunks[2])

                if stats is None:
                    response = f"Поки що за вашим запитом ”{chunks[2]}” в області ”{chunks[1]}” нічого не знайдено"
                else:
                    response = (
                        f"Всього закупівель: {stats['count']} на суму {stats['total']}\nМінімальна ціна: {stats['min']}"
                        + f"\nМаксимальна ціна: {stats['max']}\nСередня ціна: {stats['avg']}\n"
                        + f"\nЗавантажити файл: {app.config['WEBHOOK_URL']}{url_for('static', filename='export.xlsx')}"  # TODO: real urls
                    )

                viber.send_messages(
                    viber_request.sender.id,
                    TextMessage(
                        text=f"{response}\nМожете підписатися на оновлення по цій групі товарів",
                        keyboard=get_viber_subscribe_kbd(chunks[1], chunks[2]),
                    ),
                )

    elif isinstance(viber_request, ViberConversationStartedRequest):
        viber.send_messages(
            viber_request.user.id,
            [
                TextMessage(
                    text="Вітаємо вас в нашому чат-боті Ціновий Вісник! Нажміть Розпочати роботу або скористайтесь Допомогою",
                    keyboard=VIBER_MENU_KBD,
                ),
            ],
        )
    elif isinstance(viber_request, ViberSubscribedRequest):
        viber.send_messages(viber_request.sender.id, [TextMessage(None, None, viber_request.get_event_type())])
    elif isinstance(viber_request, ViberFailedRequest):
        app.logger.warning("client failed receiving message. failure: {viber_request}")

    return Response(status=200)


if __name__ == "__main__":

    def set_webhook(viber):
        app.logger.info("Setting webhook")
        viber.set_webhook(app.config["WEBHOOK_URL"])

    scheduler = sched.scheduler(time.time, time.sleep)
    scheduler.enter(5, 1, set_webhook, (viber,))
    t = threading.Thread(target=scheduler.run)
    t.start()

    procurements = db["procurements"]

    app.run(host="0.0.0.0", debug=True)
