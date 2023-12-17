# %%
# ruff: noqa: E402
# pylint: disable=wrong-import-position missing-module-docstring invalid-name missing-function-docstring too-many-lines

# %%
import os

# %% [markdown]
# ## 1. The Investigator
#
# Sarah brought a cashier over. She said, “Joe here says that one of our
# customers is a skilled private investigator.”
#
# Joe nodded, “They showed me their business card, and that’s what it said.
# Skilled Private Investigator. And their phone number was their last name
# spelled out. I didn’t know what that meant, but apparently before there were
# smartphones, people had to remember phone numbers or write them down. If you
# wanted a phone number that was easy-to-remember, you could get a number that
# spelled something using the letters printed on the phone buttons: like 2 has
# “ABC”, and 3 “DEF”, etc. And I guess this person had done that, so if you
# dialed the numbers corresponding to the letters in their name, it would call
# their phone number!
#
# “I thought that was pretty cool. But I don’t remember their name, or anything
# else about them for that matter. I couldn’t even tell you if they were male or
# female.”
#
# Sarah said, “This person seems like they are skilled at investigation. I need
# them to find Noah’s rug before the Hanukkah dinner. I don’t know how to
# contact them, but apparently they shop here at Noah’s Market.”
#
# She nodded at the [USB drive](https://hanukkah.bluebird.sh/5784-speedrun/data)
# in your hand.
#
# “Can you find this investigator’s phone number?”

# %%
password = 5783 - 6
print(password)

# %%
os.system(f"unzip -P {password} noahs-5784-speedrun-csv")

# %%
from enum import Enum

import numpy as np
import pandas as pd
import pyperclip
from IPython.display import display

# %%
customers = pd.read_csv(
    "5784-speedrun/noahs-customers.csv.zip", parse_dates=["birthdate"]
)
orders = pd.read_csv(
    "5784-speedrun/noahs-orders.csv.zip", parse_dates=["ordered", "shipped"]
)
orders_items = pd.read_csv("5784-speedrun/noahs-orders_items.csv.zip")
products = pd.read_csv("5784-speedrun/noahs-products.csv.zip")

# %%
os.system("rm -rf 5784-speedrun")


# %%
def answer(df: pd.DataFrame | pd.Series) -> str:
    r"""
    Returns answer, in this case, the phone number and copies to clipboard.

    Parameters
    ----------
    df : Union[pd.DataFrame, pd.Series]

    Returns
    -------
    str
    """
    result = df["phone"].unique()[0]
    pyperclip.copy(result)
    return result


# %%
def translate_char_to_phone_num(char: str) -> int:
    r"""
    Translate characters to phone number.

    Parameters
    ----------
    chr : str

    Returns
    -------
    int
    """
    match char:
        case "a" | "b" | "c":
            digit = 2
        case "d" | "e" | "f":
            digit = 3
        case "g" | "h" | "i":
            digit = 4
        case "j" | "k" | "l":
            digit = 5
        case "m" | "n" | "o":
            digit = 6
        case "p" | "q" | "r" | "s":
            digit = 7
        case "t" | "u" | "v":
            digit = 8
        case "w" | "x" | "y" | "z":
            digit = 9
    return digit


# %%
def one_the_investigator(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.loc[~df["name"].str.endswith(("II", "III", "IV", "Jr."))]
        .assign(
            last_name=df["name"].str.split().str[-1].str.lower(),
            last_name_len=lambda d: d["last_name"].str.len(),
        )
        .query("last_name_len == 10")
        .assign(
            name_char=lambda df: df["last_name"].map(list),
            phone_num=lambda df: df["name_char"].map(
                lambda _: "".join([str(translate_char_to_phone_num(x)) for x in _])
            ),
            test=lambda d: (
                d["phone_num"].str.slice(stop=3)
                + "-"
                + d["phone_num"].str.slice(start=3, stop=6)
                + "-"
                + d["phone_num"].str.slice(start=6)
            ),
        )
        .loc[lambda d: d["phone"] == d["test"], customers.columns]
        .set_index("customerid")
    )


# %%
customers.pipe(one_the_investigator)

# %%
customers.pipe(one_the_investigator).pipe(answer)


# %% [markdown]
# ## 2. The Contractor
#
# Thanks to your help, Sarah called the investigator that afternoon. The
# investigator went directly to the cleaners to see if they could get any more
# information about the unclaimed rug.
#
# While they were out, Sarah said, “I tried cleaning the rug myself, but there
# was this snail on it that always seemed to leave a trail of slime behind it. I
# spent a few hours cleaning it, and the next day the slime trail was back.”
#
# When the investigator returned, they said, “Apparently, this cleaner had a
# special projects program, where they outsourced challenging cleaning projects
# to industrious contractors. As they’re right across the street from Noah’s,
# they usually talked about the project over coffee and bagels at Noah’s before
# handing off the item to be cleaned. The contractors would pick up the tab and
# expense it, along with their cleaning supplies.
#
# “So this rug was apparently one of those special projects. The claim ticket
# said ‘2017 DS’. ‘2017’ is the year the item was brought in, and ‘DS’ is the
# initials of the contractor.
#
# “But they stopped outsourcing a few years ago, and don’t have contact
# information for any of these workers anymore.”
#
# Sarah first seemed hopeless, and then glanced at the [USB
# drive](https://hanukkah.bluebird.sh/5784-speedrun/data) you had just put back
# in her hand. She said, “I know it’s a long shot, but is there any chance you
# could find their phone number?”


# %%
def two_the_contractor(
    customers_df: pd.DataFrame = customers,
    orders_df: pd.DataFrame = orders,
    orders_items_df: pd.DataFrame = orders_items,
    products_df: pd.DataFrame = products,
    initials: str = "DS",
) -> pd.DataFrame:
    return (
        (
            customers_df.replace([" II", " III", " IV", " Jr."], "", regex=True)
            .assign(
                initials=lambda d: (
                    d["name"].str.split(" ").str[0].str[0]
                    + d["name"].str.split(" ").str[-1].str[0]
                )
            )
            .merge(
                orders_df.loc[
                    (orders_df["ordered"].dt.year == 2017)
                    & ((orders_df["ordered"] - orders_df["shipped"]).dt.seconds <= 60)
                ],
                on="customerid",
            )
            .loc[lambda d: d["initials"] == initials]
            .merge(orders_items_df, on="orderid")
            .merge(
                products_df.loc[
                    products_df["desc"].str.contains("coffee|bagel&clean", case=False)
                ],
                on="sku",
            )
            .filter(customers.columns)
        )
        .drop_duplicates()
        .set_index("customerid")
    )


# %%
customers.pipe(two_the_contractor)

# %%
customers.pipe(two_the_contractor).pipe(answer)

# %% [markdown]
# ## 3. The Neighbor
#
# Sarah and the investigator were very impressed with your data skills, as you
# were able to figure out the phone number of the contractor. They called up the
# cleaning contractor straight away and asked about the rug.
#
# “Oh, yeah, I did some special projects for them a few years ago. I remember
# that rug unfortunately. I managed to clean one section, which revealed a giant
# spider that startled me whenever I tried to work on it.
#
# “I already had a fear of spiders before this, but this spider was so realistic
# that I had a hard time making any more progress. I kept expecting the cleaners
# would call for the rug, but they never did. I felt so bad about it, I couldn’t
# face them, and of course they never gave me another project.
#
# “At last I couldn’t deal with the rug taking up my whole bathtub, so I gave it
# to this guy who lived in my neighborhood. He said that he was naturally
# balanced because he was a Libra born in the year of the Goat, so maybe he was
# able to clean it.
#
# “I don’t remember his name. Last time I saw him, he was leaving the subway and
# carrying a bag from Noah’s. I swore I saw a spider on his hat.”
#
# Can you find the phone number of the person that the contractor gave the rug
# to?

# %%
ZODIAC_LINK = "https://en.wikipedia.org/wiki/Astrological_sign"

# %%
zodiac = pd.read_html(ZODIAC_LINK)[0]

# %%
zodiac


# %%
class ZodiacSign(Enum):
    """
    Represents the different zodiac signs.

    Explanation
    -----------
    This class defines an enumeration of the twelve zodiac signs.
    """

    Aries = "Aries"
    Taurus = "Taurus"
    Gemini = "Gemini"
    Cancer = "Cancer"
    Leo = "Leo"
    Virgo = "Virgo"
    Libra = "Libra"
    Scorpio = "Scorpio"
    Sagittarius = "Sagittarius"
    Capricorn = "Capricorn"
    Aquarius = "Aquarius"
    Pisces = "Pisces"


# %%
class ChineseZodiac(Enum):
    """
    Represents the Chinese zodiac signs.

    Explanation
    -----------
    This class defines an enumeration of the twelve Chinese zodiac signs and
    return the respective Wikipedia link.
    """

    Rat = "https://en.wikipedia.org/wiki/Rat_(zodiac)"
    Ox = "https://en.wikipedia.org/wiki/Ox_(zodiac)"
    Tiger = "https://en.wikipedia.org/wiki/Tiger_(zodiac)"
    Rabbit = "https://en.wikipedia.org/wiki/Rabbit_(zodiac)"
    Dragon = "https://en.wikipedia.org/wiki/Dragon_(zodiac)"
    Snake = "https://en.wikipedia.org/wiki/Snake_(zodiac)"
    Horse = "https://en.wikipedia.org/wiki/Horse_(zodiac)"
    Goat = "https://en.wikipedia.org/wiki/Goat_(zodiac)"
    Monkey = "https://en.wikipedia.org/wiki/Monkey_(zodiac)"
    Rooster = "https://en.wikipedia.org/wiki/Rooster_(zodiac)"
    Dog = "https://en.wikipedia.org/wiki/Dog_(zodiac)"
    Pig = "https://en.wikipedia.org/wiki/Pig_(zodiac)"


# %%
def zodiac_characteristics(
    zodiac_df: pd.DataFrame = zodiac, zodiac_sign: ZodiacSign = ZodiacSign.Cancer
) -> dict[str, list[str | int]]:
    """
    Extracts zodiac characteristics from a DataFrame based on the specified
    zodiac sign.

    Parameters
    ----------
    zodiac_df : pd.DataFrame, optional
        The DataFrame containing zodiac information. By default, uses a
        predefined zodiac DataFrame.
    zodiac_sign : ZodiacSign, optional
        The zodiac sign for which characteristics are extracted. By default,
        uses ZodiacSign.Cancer.

    Returns
    -------
    dict[str, list[str | int]]
        A dictionary containing zodiac characteristics:
        - 'dates': List of date strings.
        - 'month': List of month values.
        - 'days': List of day values.
    """
    return (
        (
            zodiac_df.loc[zodiac_df["Sign"] == zodiac_sign.value]
            .filter(like="Sun", axis=1)
            .T
        )
        .set_axis(["dates"], axis="columns")
        .assign(
            timestamp_fmt=lambda d: pd.to_datetime(d["dates"], format="%d %B"),
            month=lambda d: d["timestamp_fmt"].dt.month,
            days=lambda d: d["timestamp_fmt"].dt.day,
        )
        .filter(regex="^(?!.*timestamp).*$")
        .to_dict("list")
    )


# %%
zodiac.pipe(zodiac_characteristics, ZodiacSign.Sagittarius)


# %%
def chinese_sign_years(
    customers_df: pd.DataFrame = customers,
    chinese_zodiac_animal: ChineseZodiac = ChineseZodiac.Rabbit,
) -> set[int]:
    """
    Retrieves the birth years of customers belonging to a specific Chinese
    zodiac sign.

    Parameters
    ----------
    customers_df : pd.DataFrame, optional
        The DataFrame containing customer information. By default, uses a
        predefined 'customers' DataFrame.
    chinese_zodiac_animal : ChineseZodiac, optional
        The Chinese zodiac animal for which to retrieve birth years.
        By default, uses the ChineseZodiac.Rabbit.

    Returns
    -------
    set[int]
        A set of birth years corresponding to customers with the specified
        Chinese zodiac sign.
    """

    tables = pd.read_html(chinese_zodiac_animal.value)

    for i, d in enumerate(tables):
        cols = list(d.columns)
        if "Start date" in cols:
            date_table_index = i

    return set(pd.to_numeric(tables[date_table_index]["Start date"].str[-4:])) & set(
        customers_df["birthdate"].dt.year
    )


# %%
chinese_sign_years(customers, ChineseZodiac.Goat)


# %%
def western_astrology_with_chinese_dates(
    western_astrology_sign: ZodiacSign = ZodiacSign.Cancer,
    chinese_astrology_animal: ChineseZodiac = ChineseZodiac.Rabbit,
) -> list[pd.Timestamp]:
    """
    Retrieves a list of timestamps representing the overlapping periods between
    Western astrology sign dates and Chinese astrology birth years.

    Parameters
    ----------
    western_astrology_sign : ZodiacSign, optional
        The Western astrology sign for which to retrieve overlapping dates.
        By default, uses ZodiacSign.Cancer.
    chinese_astrology_animal : ChineseZodiac, optional
        The Chinese astrology animal for which to retrieve overlapping dates.
        By default, uses ChineseZodiac.Rabbit.

    Returns
    -------
    list[pd.Timestamp]
        A list of timestamps representing the overlapping periods between the
        specified Western astrology sign and Chinese astrology birth years.
    """

    sign_dict: dict[str, list[str | int]] = zodiac_characteristics(
        zodiac_sign=western_astrology_sign
    )
    animal_years: set[int] = chinese_sign_years(
        chinese_zodiac_animal=chinese_astrology_animal
    )

    months: list[int] = sign_dict["month"]
    start_month, end_month = months

    days: list[int] = sign_dict["days"]
    start_day, end_day = days

    return [
        pd.Period.to_timestamp(date)
        for date in np.concatenate(
            [
                pd.period_range(
                    start=pd.Timestamp(f"{start_month}/{start_day}/{animal_year}"),
                    end=pd.Timestamp(f"{end_month}/{end_day}/{animal_year}"),
                )
                for animal_year in sorted(animal_years)
            ],
            axis=0,
        )
    ]


# %%
def three_the_neighbor(
    customers_df: pd.DataFrame = customers,
    western_astrology_sign: ZodiacSign = ZodiacSign.Libra,
    chinese_astrology_animal: ChineseZodiac = ChineseZodiac.Goat,
) -> pd.DataFrame:
    dates = western_astrology_with_chinese_dates(
        western_astrology_sign, chinese_astrology_animal
    )
    the_contractor = two_the_contractor()
    return (
        customers_df.loc[customers_df["birthdate"].isin(dates)]
        .assign(
            zip_code=lambda d: d["citystatezip"].str[-5:],
            neighbor=lambda d: d["zip_code"]
            == the_contractor["citystatezip"].str[-5:].iloc[0],
        )
        .loc[lambda d: d["neighbor"]]
        .filter(customers_df.columns)
    )


# %%
customers.pipe(three_the_neighbor).pipe(display)

# %%
customers.pipe(three_the_neighbor).pipe(answer)


# %%
del zodiac


# %% [markdown]
# ## 4. The Early Bird
#
# The investigator called the phone number you found and left a message, and a
# man soon called back:
#
# “Wow, that was years ago! It was quite an elegant tapestry.
#
# “It took a lot of patience, but I did manage to get the dirt out of one
# section, which uncovered a superb owl. I put it up on my wall, and sometimes
# at night I swear I could hear the owl hooting.
#
# “A few weeks later my bike chain broke on the way home, and I needed to get it
# fixed before work the next day. Thankfully, this woman I met on Tinder came
# over at 5am with her bike chain repair kit and some pastries from Noah’s.
# Apparently she liked to get up before dawn and claim the first pastries that
# came out of the oven.
#
# “I didn’t have any money or I would’ve paid her for her trouble. She really
# liked the tapestry, though, so I wound up giving it to her.
#
# “I don’t remember her name or anything else about her.”
#
# Can you find the bicycle fixer’s phone number?


# %%
def earlybird_customer_id(
    orders_df: pd.DataFrame = orders, orders_items_df: pd.DataFrame = orders_items
) -> int:
    """
    Find the customer ID who placed an early-bird order.

    Parameters
    ----------
    orders_df : pd.DataFrame, optional
        DataFrame containing order information, by default orders.
    orders_items_df : pd.DataFrame, optional
        DataFrame containing order items information, by default orders_items.

    Returns
    -------
    int
        The customer ID who placed the early-bird order meeting the specified
        criteria.
    """
    return (
        orders_df.loc[
            (orders_df["ordered"].dt.hour < 5) & (orders_df["shipped"].dt.hour < 5)
        ]
        .merge(orders_items_df, on="orderid")
        .loc[lambda d: (d["sku"].str[:3] == "BKY") & (d["qty"] > 1), "customerid"]
        .mode()
        .iloc[0]
    )


# %%
orders.pipe(earlybird_customer_id)


# %%
def four_the_early_bird(customers_df: pd.DataFrame = customers) -> pd.DataFrame:
    earlybird_customer: int = earlybird_customer_id()
    return customers_df.loc[customers_df["customerid"] == earlybird_customer]


# %%
customers.pipe(four_the_early_bird).pipe(display)

# %%
customers.pipe(four_the_early_bird).pipe(answer)

# %% [markdown]
# ## 5. The Cat Lady
#
# “Yes, I did have that tapestry for a little bit. I even cleaned a blotchy
# section that turned out to be a friendly koala.
#
# “But it was still really dirty, so when I was going through a Marie Kondo
# phase, I decided it wasn’t sparking joy anymore.
#
# “I listed it on Freecycle, and a woman came to pick it up. She was wearing a
# ‘Noah’s Market’ sweatshirt, and it was just covered in cat hair. When I
# suggested that a clowder of cats might ruin such a fine tapestry, she looked
# at me funny. She said “I only have ten or eleven cats, and anyway they are
# getting quite old now, so I doubt they’d care about some old rug.”
#
# “It took her 20 minutes to stuff the tapestry into some plastic bags she
# brought because it was raining. I spent the evening cleaning my apartment.”
#
# What’s the phone number of the woman from Freecycle?


# %%
def top_customers_id_cat_products(
    customers_df: pd.DataFrame = customers,
    orders_df: pd.DataFrame = orders,
    orders_items_df: pd.DataFrame = orders_items,
    products_df: pd.DataFrame = products,
    # new_york_borough: str = "Staten Island",
) -> pd.Series:
    """
    Returns the mode (most frequent) customer ID of customers in a specified New
    York borough who have purchased products containing the term "cat" in their
    description.

    Parameters
    ----------
    customers_df : pd.DataFrame, optional
        DataFrame containing customer information, by default customers
    orders_df : pd.DataFrame, optional
        DataFrame containing order information, by default orders
    orders_items_df : pd.DataFrame, optional
        DataFrame containing order items information, by default orders_items
    products_df : pd.DataFrame, optional
        DataFrame containing product information, by default products
    new_york_borough : str, optional
        Name of the New York borough to filter customers, by default "Staten
        Island"

    Returns
    -------
    pd.Series
        Series containing the mode customer ID of customers meeting the
        specified criteria.
    """
    return pd.Series(
        customers_df.merge(orders_df, on="customerid")
        .merge(orders_items_df, on="orderid")
        .merge(products_df, on="sku")
        .loc[
            lambda d: d["sku"].isin(
                products_df.loc[
                    products_df["desc"].str.contains("senior cat", case=False)
                ]
                .agg({"sku": "unique"})
                .squeeze()
            )
        ]
        .agg({"customerid": "mode"})
        .squeeze()
    )


# %%
def five_the_cat_lady(customers_df: pd.DataFrame = customers) -> pd.DataFrame:
    """
    Filters and returns a DataFrame containing information about customers who
    are identified as top customers based on their purchase history of products
    containing the term "cat" in the description.

    Parameters
    ----------
    customers_df : pd.DataFrame, optional
        DataFrame containing customer information, by default customers

    Returns
    -------
    pd.DataFrame
        DataFrame containing information about customers identified as top
        customers based on their purchase history of "cat" products.
    """
    top_customers_id_cat_products_series: pd.Series = top_customers_id_cat_products()
    return customers_df.loc[
        customers_df["customerid"].isin(top_customers_id_cat_products_series)
    ]


# %%
customers.pipe(five_the_cat_lady).pipe(display)

# %%
customers.pipe(five_the_cat_lady).pipe(answer)


# %% [markdown]
# ## 6. The Bargain Hunter
#
# “Why yes, I did have that rug for a little while in my living room! My cats
# can’t see a thing but they sure chased after the squirrel on it like it was
# dancing in front of their noses.
#
# “It was a nice rug and they were surely going to ruin it, so I gave it to my
# cousin, who was moving into a new place that had wood floors.
#
# “She refused to buy a new rug for herself–she said they were way too
# expensive. She’s always been very frugal, and she clips every coupon and shops
# every sale at Noah’s Market. In fact I like to tease her that Noah actually
# loses money whenever she comes in the store.
#
# “I think she’s been taking it too far lately though. Once the subway fare
# increased, she stopped coming to visit me. And she’s really slow to respond to
# my texts. I hope she remembers to invite me to the family reunion next year.”
#
# Can you find her cousin’s phone number?


# %%
def six_the_bargain_hunter(
    customers_df: pd.DataFrame = customers,
    products_df: pd.DataFrame = products,
    orders_df: pd.DataFrame = orders,
    orders_items_df: pd.DataFrame = orders_items,
) -> pd.DataFrame:
    """
    Filters a DataFrame of customers to include only those who are identified as
    savvy bargain hunters.

    Parameters
    ----------
    customers_df : pd.DataFrame, optional
        DataFrame containing customer information, by default customers.
    products_df : pd.DataFrame, optional
        DataFrame containing product information, by default products.
    orders_df : pd.DataFrame, optional
        DataFrame containing order information, by default orders.
    orders_items_df : pd.DataFrame, optional
        DataFrame containing order items information, by default orders_items.

    Returns
    -------
    pd.DataFrame
        DataFrame containing information about customers identified as savvy
        bargain hunters.

    Notes
    -----
    This function identifies customers who are considered bargain hunters based
    on their purchase behavior. The criteria include purchasing products at a
    unit price less than or equal to the wholesale cost. The resulting DataFrame
    includes customer information for those who meet the specified criteria.
    """
    bh_customer_id = (
        orders_items_df.groupby(["sku", "orderid"], as_index=False)
        .agg({"unit_price": "min"})
        .merge(products_df, on="sku")
        .loc[lambda d: d["unit_price"] <= d["wholesale_cost"]]
        .merge(orders_df, on="orderid")
        .merge(customers_df, on="customerid")
        .agg({"customerid": "mode"})
        .iloc[0]
    )

    return customers_df.loc[customers_df["customerid"].isin(bh_customer_id)]


# %%
customers.pipe(six_the_bargain_hunter).pipe(display)

# %%
customers.pipe(six_the_bargain_hunter).pipe(answer)

# %% [markdown]
# ## 7. The Meet Cute
#
# “Oh that tapestry, with the colorful toucan on it! I’ll tell you what happened
# to it.
#
# “One day, I was at Noah’s Market, and I was just about to leave when someone
# behind me said ‘Miss! You dropped something!’
#
# “Well I turned around to see this cute guy holding an item I had bought. He
# said, ‘I got the same thing!’ We laughed about it and wound up swapping items
# because I wanted the color he got. We had a moment when our eyes met and my
# heart stopped for a second. I asked him to get some food with me and we spent
# the rest of the day together.
#
# “Before long I moved into his place, but the romance faded quickly, as he
# wasn’t the prince I imagined. I left abruptly one night, forgetting the
# tapestry on his wall. But by then, it symbolized our love, and I wanted
# nothing more to do with it. For all I know, he still has it.”
#
# Can you figure out her ex-boyfriend’s phone number?


# %%
def date_hour_mm(orders_df: pd.DataFrame = orders) -> pd.DataFrame:
    """
    Extracts and formats the date and hour from the 'ordered' column in a
    DataFrame of order information.

    Parameters
    ----------
    orders_df : pd.DataFrame, optional
        DataFrame containing order information, by default orders.

    Returns
    -------
    pd.DataFrame
        DataFrame with an additional 'date_hour' column representing the
        formatted date and hour.

    Notes
    -----
    This function extracts the date and hour information from the 'ordered'
    column and adds a new column 'date_hour' to the DataFrame. The 'date_hour'
    column is formatted as 'MM/DD/YYYY HH MM' to represent the month, day,
    year, and hour of each order.
    """
    return orders_df.assign(
        date_hour=lambda df: df["ordered"].dt.strftime("%m/%d/%Y %H:%M")
    )


# %%
def color_agnostic_item_name(products_df: pd.DataFrame = products) -> pd.DataFrame:
    """
    Generates a new column for items, removing color information to create
    color-agnostic names.

    Parameters
    ----------
    products_df : pd.DataFrame, optional
        DataFrame containing product information, by default products.

    Returns
    -------
    pd.DataFrame
        DataFrame with an additional 'desc_color_agnostic' column representing
        color-agnostic item names.

    Notes
    -----
    This function creates a new column 'desc_color_agnostic' by removing color
    information from the 'desc' column. It ensures that item names are
    represented without specific color details.

    Examples
    --------
    >>> color_agnostic_item_name(df)
    # Returns a DataFrame with 'desc_color_agnostic' column:
    # 'Manual Mixer (orange)' -> 'Manual Mixer'

    """
    return (
        products_df.assign(
            color_list=lambda df: df["desc"]
            .str.split("(")
            .str[1]
            .str.replace(")", "")
            .str.split()
        )
        .assign(color=lambda df: df["color_list"].str[0])
        .dropna()
        .loc[lambda df: df["color"].str[0].str.islower()]
        .drop(columns=["color_list"])
        .assign(desc_color_agnostic=lambda df: df["desc"].str.split(r" \(").str[0])
    )


# %%
def filter_in_store_orders(orders_df: pd.DataFrame = orders) -> pd.DataFrame:
    """
    Filters out in-store orders from a DataFrame of order information.

    Parameters
    ----------
    orders_df : pd.DataFrame, optional
        DataFrame containing order information, by default orders.

    Returns
    -------
    pd.DataFrame
        DataFrame with in-store orders removed.

    Notes
    -----
    This function filters out orders that were bought in-store by comparing the
    'ordered' and 'shipped' columns. Only orders with distinct 'ordered' and
    'shipped' timestamps are retained.

    Example
    -------
    >>> filter_in_store_orders(df)
    # Returns a DataFrame with in-store orders removed.
    """
    return orders_df.loc[orders_df["ordered"] == orders_df["shipped"]]


# %%
def bargain_hunter_in_store_color_items(
    products_df: pd.DataFrame = products,
    orders_items_df: pd.DataFrame = orders_items,
    orders_df: pd.DataFrame = orders,
) -> pd.DataFrame:
    return (
        products_df.pipe(color_agnostic_item_name)
        .merge(orders_items_df, on="sku")
        .merge(orders_df, on="orderid")
        .loc[
            lambda d: (
                d["customerid"].isin(six_the_bargain_hunter().loc[:, "customerid"])
            )
        ]
        .pipe(filter_in_store_orders)
        .pipe(date_hour_mm)
    )


# %%
def the_order_of_the_meet(
    orders_df: pd.DataFrame = orders,
    orders_items_df: pd.DataFrame = orders_items,
    products_df: pd.DataFrame = products,
) -> pd.DataFrame:
    """
    Combines and filters various DataFrames to create a comprehensive dataset
    of order information.

    Parameters
    ----------
    orders_df : pd.DataFrame, optional
        DataFrame containing order information, by default orders.
    orders_items_df : pd.DataFrame, optional
        DataFrame containing order items information, by default orders_items.
    products_df : pd.DataFrame, optional
        DataFrame containing product information, by default products.

    Returns
    -------
    pd.DataFrame
        DataFrame with a filtered and merged dataset representing comprehensive
        order information.
    """
    bargain_hunter = bargain_hunter_in_store_color_items()
    return (
        orders_df.pipe(filter_in_store_orders)
        .merge(orders_items_df, on="orderid")
        .pipe(date_hour_mm)
        .merge(products_df.pipe(color_agnostic_item_name), on="sku")
        .merge(
            bargain_hunter.loc[:, ["desc_color_agnostic", "date_hour"]],
            on=["desc_color_agnostic", "date_hour"],
        )
        .loc[lambda df: ~df["customerid"].isin(bargain_hunter["customerid"])]
        .filter(bargain_hunter.columns)
        .pipe(
            lambda cute_guy: pd.concat(
                [
                    bargain_hunter.loc[
                        lambda b: (
                            b["desc_color_agnostic"].isin(
                                cute_guy["desc_color_agnostic"]
                            )
                        )
                        & (b["date_hour"].isin(cute_guy["date_hour"]))
                    ],
                    cute_guy,
                ]
            )
        )
    )


# %%
orders.pipe(the_order_of_the_meet)


# %%
def seven_the_meet_cute(customers_df: pd.DataFrame = customers) -> pd.DataFrame:
    the_meet = the_order_of_the_meet()
    the_couple_customer_ids = the_meet["customerid"]
    the_bargain_hunter = six_the_bargain_hunter()["customerid"]
    the_meet_cute_id = set(the_couple_customer_ids) - set(the_bargain_hunter)
    return customers_df.loc[customers_df["customerid"].isin(list(the_meet_cute_id))]


# %%
customers.pipe(seven_the_meet_cute).pipe(display)

# %%
customers.pipe(seven_the_meet_cute).pipe(answer)


# %% [markdown]
# ## 8. The Collector
#
# “Oh that damned woman! She moved in, clogged my bathtub, left her coupons all
# over the kitchen, and then just vanished one night without leaving so much as
# a note.
#
# Except she did leave behind that nasty carpet. I spent months cleaning one
# corner, only to discover a snake hiding in the branches! I knew then that she
# was never coming back, and I had to get it out of my sight.
#
# “Well, I don’t have any storage here, and it didn’t seem right to sell it, so
# I gave it to my sister. She wound up getting a newer and more expensive
# carpet, so she gave it to an acquaintance of hers who collects all sorts of
# junk. Apparently he owns an entire set of Noah’s collectibles! He probably
# still has the carpet, even.
#
# “My sister is away for the holidays, but I can have her call you in a few
# weeks.”
#
# The family dinner is tonight! Can you find the collector’s phone number in
# time?


# %%
def eight_the_collector(
    customers_df: pd.DataFrame = customers,
    orders_df: pd.DataFrame = orders,
    orders_items_df: pd.DataFrame = orders_items,
):
    top_buyer = (
        orders_df.merge(orders_items_df, on="orderid")["customerid"]
        .value_counts()
        .idxmax()
    )

    return customers_df.loc[customers_df["customerid"] == top_buyer]


# %%
customers.pipe(eight_the_collector).pipe(display)

# %%
customers.pipe(eight_the_collector).pipe(answer)

# %% [markdown]
# ## 9. Epilogue
#
# “Oh yes, that magnificant Persian carpet! An absolute masterpiece, with a
# variety of interesting animals congregating around a Tree of Life. As a
# collector, I couldn’t believe when it fell into my lap.
#
# “A friend of mine had taken it off her brother’s hands, and she didn’t know
# what to do with it. I saw her one day, and she was about to put an old rug out
# at the curb. It looked like it had been through a lot, but it was remarkably
# not that dirty. It still took quite a bit of effort and no small amount of rug
# cleaner, but ultimately I managed to get the last bits of grime out of it.
#
# “I actually live right down the street from Noah’s Market–I’m a huge fan and I
# shop there all the time! I even have a one-of-a-kind scale model of Noah’s Ark
# that makes a complete set of Noah’s collectibles.
#
# “I would love for Noah to have his rug once again to enjoy.”
#
# ![](hod_finish_speedrun.gif)
