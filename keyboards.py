from dicts import REGIONS, PRODUCT_CATEGORIES, SUBSCRIPTION_TYPES


def viber_button(text, action_body, params=None):
    default_params = {
        "Columns": 3,
        "Rows": 1,
        "BgColor": "#e6f5ff",
        "BgLoop": True,
        "ActionType": "reply",
        "ActionBody": action_body,
        "ReplyType": "message",
        "Text": text,
    }

    if params is not None:
        default_params.update(params)

    return default_params


VIBER_MENU_KBD = {
    "Type": "keyboard",
    "Buttons": [
        viber_button("Розпочати роботу", "start"),
        viber_button("Допомога", "help"),
        viber_button("Ваші підписки", "subscriptions"),
    ],
}


VIBER_REGIONS_KBD = {
    "Type": "keyboard",
    "Buttons": [viber_button(f"{v}", f"region:{v}") for v in sorted(list(set(REGIONS.values())))],
}


def get_viber_categories_kbd(region):
    return {
        "Type": "keyboard",
        "Buttons": [
            viber_button(f"{v.title()}", f"product_category:{region}:{v}")
            for v in sorted(list(set(PRODUCT_CATEGORIES.values())))
        ],
    }


def get_viber_subscribe_kbd(region, product_category):
    return {
        "Type": "keyboard",
        "Buttons": 
        # [viber_button("Скачати файл", "export"), {"ActionType": "open-url",
        #     "ActionBody": ""}] + 
        [
            viber_button(f"{k}", f"subscribe:{region}:{product_category}:{period}")
            for k, period in SUBSCRIPTION_TYPES.items()
        ] + [viber_button("Ні, дякую", "start")],
    }
