# %% [markdown]
# # [Noah's Rug](https://www.whereinthedata.com/noahsrug/)
#
# ## 0. Noah's Market
#
# Welcome to "Noah's Market", a bustling mom-and-pop everything store in
# Manhattan. In recent years it’s become quite an operation, but Noah and family
# still own and manage it.
#
# This morning, while waiting for your breakfast bagel, your Aunt Sarah pulled
# you aside in a hustle.
#
# "You know how Grandpa Noah has been talking recently about that rug we used to
# have?"
#
# She looked over at Noah, who was talking to a customer: "Such a beautiful
# carpet, with the most intricate design! I miss having it in my den. It has
# this vibrant beehive buzzing in the corner…"
#
# Sarah explained, "He entrusted me with that rug when he was remodeling his den
# a few years ago. It was so old and filthy, that I sent it to the cleaners, but
# then I completely forgot about it. Now, with him retiring and me taking over
# the store, he wants it back. So yesterday I freaked out and combed my
# apartment, and I finally found a claim ticket saying, 'All items must be
# picked up within 90 days.' At the cleaners, they didn't have the rug, just the
# other half of the ticket."
#
# Sarah added, "I need to find that rug before your grandpa comes over on the
# last night of Hanukkah. I have an idea involving a customer in our
# database--we can look up customers by name, of course, but this one is a
# little tricky because I don't know their name exactly. So I called Alex, you
# know how he set up the database when he was just a freshman? But now that he's
# working for a big corporation, little Alexander Carpenter has no time to help
# his mother."
#
# She brings you into the back office and shows you a computer terminal.
#
# "But I was thinking, maybe you could do it? I mean here is the terminal where
# Alex did all of his stuff with the database. I can never find the time to
# figure it out, so you'll have to call Alex to learn how to use it."
#
# Sarah sighs and goes to talk with one of the cashiers.
#
# Can you find your cousin's phone number?

# %%
from datetime import date, datetime
from enum import Enum
from zipfile import ZipFile

import pandas as pd
import polars as pl
import polars.selectors as cs
import pyperclip


def get_zipped_csv(zipped_filename: str) -> bytes:
    return ZipFile(zipped_filename).open(zipped_filename[:-4]).read()


def answer(df: pl.DataFrame) -> str:
    r"""
    Returns answer, in this case, the phone number and copies to clipboard.

    Parameters
    ----------
    df : Union[pd.DataFrame, pd.Series]

    Returns
    -------
    str
    """
    result = df.select("phone").item()
    pyperclip.copy(result)
    return result


def preview_dfs() -> None:
    for df in [orders, customers, products, orders_items]:
        print(df.pipe(z_namestr))
        df.head().collect().pipe(z_classy_print)


# %%
products = pl.scan_csv(get_zipped_csv("noahs-products.csv.zip"))
customers = pl.scan_csv(get_zipped_csv("noahs-customers.csv.zip"), try_parse_dates=True)
orders = pl.scan_csv(get_zipped_csv("noahs-orders.csv.zip"), try_parse_dates=True)
orders_items = pl.scan_csv(get_zipped_csv("noahs-orders_items.csv.zip"))


# %%
def one(customers_df: pl.LazyFrame) -> pl.DataFrame:
    return customers_df.filter(
        pl.col("name").str.contains("Alexander Carpenter")
    ).collect()


# %%
customers.pipe(one)

# %%
_.pipe(answer)


# %% [markdown]
# ```sql
# SELECT phone
# FROM CUSTOMERS
# WHERE name LIKE "Alexander Carpenter"
# ```

# %% [markdown]
# ## 1. The Investigator
#
# You called Alex, and Alex gave you the lowdown on the terminal.
#
# "It's just SQLite," he said. "It's not that hard to figure out. I even made a
# nice little interface for it--everything you need to know should be in the
# Help tab. Anyway I have to go, I'm trying to solve the latest Advent of Code
# puzzle using CSS."
#
# Alex hung up just as Sarah brought a cashier into the office.
#
# She said, "Joe here says that one of our customers is a skilled private
# investigator."
#
# Joe nodded, "They showed me their business card, and that's what it said.
# Skilled Private Investigator. And their phone number was their last name
# spelled out. I didn't know what that meant, but apparently before there were
# smartphones, people had to remember phone numbers or write them down. If you
# wanted a phone number that was easy-to-remember, you could get a number that
# spelled something using the letters printed on the phone buttons: like 2 has
# "ABC", and 3 "DEF", etc. And I guess this person had done that, so if you
# dialed the numbers corresponding to the letters in their name, it would call
# their phone number!
#
# "I thought that was pretty cool. But I don't remember their name, or anything
# else about them for that matter. I couldn't even tell you if they were male or
# female."
#
# Sarah said, "This person seems like they are skilled at investigation. I need
# them to find Noah's rug before the Hanukkah dinner. I don't know how to
# contact them, but apparently they shop here at Noah's Market."
#
# "So can you find this investigator's phone number?"


# %%
def translate_to_phone_num(text: str) -> str:
    """
    Translate string characters to phone numbers.

    Parameters
    ----------
    text : str
        Input string to be translated to phone numbers

    Returns
    -------
    str
        String of digits corresponding to the input text

    Examples
    --------
    >>> translate_to_phone_num("a")
    "2"
    >>> translate_to_phone_num("hello")
    "43556"
    """

    def translate_char(char: str) -> str:
        char = char.lower()
        match char:
            case "a" | "b" | "c":
                digit = "2"
            case "d" | "e" | "f":
                digit = "3"
            case "g" | "h" | "i":
                digit = "4"
            case "j" | "k" | "l":
                digit = "5"
            case "m" | "n" | "o":
                digit = "6"
            case "p" | "q" | "r" | "s":
                digit = "7"
            case "t" | "u" | "v":
                digit = "8"
            case "w" | "x" | "y" | "z":
                digit = "9"
            case _:
                digit = char
        return digit

    return "".join(translate_char(char) for char in text)


# %%
def the_investigator(customers_df: pl.LazyFrame) -> pl.DataFrame:
    return (
        customers_df.filter(
            ~pl.col("name").str.contains_any(["II", "III", "IV", "Jr."])
        )
        .with_columns(
            lastname_translated=(
                pl.col("name")
                .str.split(by=" ")
                .list.get(-1)
                .str.to_lowercase()
                .map_elements(translate_to_phone_num, return_dtype=pl.String)
            )
        )
        .with_columns(
            phone_test=(
                pl.col("lastname_translated").str.slice(0, length=3)
                + "-"
                + pl.col("lastname_translated").str.slice(3, length=3)
                + "-"
                + pl.col("lastname_translated").str.slice(6, length=4)
            )
        )
        .filter(pl.col("phone") == pl.col("phone_test"))
        .select(customers_df.collect_schema().names())
        .collect()
    )


# %%
customers.pipe(the_investigator)

# %%
_.pipe(answer)


# %% [markdown]
# ## 2. The Contractor
#
# Thanks to your help, Sarah called the investigator that afternoon. The
# investigator went directly to the cleaners to see if they could get any more
# information about the unclaimed rug.
#
# While they were out, Sarah said, "I tried cleaning the rug myself, but there
# was this snail on it that always seemed to leave a trail of slime behind it. I
# spent a few hours cleaning it, and the next day the slime trail was back."
#
# When the investigator returned, they said, "Apparently, this cleaner had a
# special projects program, where they outsourced challenging cleaning projects
# to industrious contractors. As they're right across the street from Noah's,
# they usually talked about the project over coffee and bagels at Noah's before
# handing off the item to be cleaned. The contractors would pick up the tab and
# expense it, along with their cleaning supplies.
#
# "So this rug was apparently one of those special projects. The claim ticket
# said '2017 DS'. '2017' is the year the item was brought in, and 'DS' is the
# initials of the contractor.
#
# "But they stopped outsourcing a few years ago, and don't have contact
# information for any of these workers anymore."
#
# Sarah first seemed hopeless, and then glanced at the terminal you were just
# trying to get away from. She said, "I know it's a long shot, but is there any
# chance you could find their phone number?"


# %%
def the_contractor(
    products_df: pl.LazyFrame,
    orders_items_df: pl.LazyFrame,
    orders_df: pl.LazyFrame,
    customers_df: pl.LazyFrame,
) -> pl.DataFrame:
    return (
        products_df.filter(
            pl.col("desc").str.contains("(?i)bagel|(?i)coffee|(?i)clean")
        )
        .join(orders_items_df, on="sku")
        .join(orders_df.filter(pl.col("ordered").dt.year() == 2017), on="orderid")
        .join(
            customers_df.filter(pl.col("name").str.contains(r"^D.*\sS.*$")),
            on="customerid",
        )
        .group_by(["name", "phone", "ordered"])
        .agg(
            bagel_coffee_clean=(pl.col("desc").str.contains("(?i)coffee").sum() > 0)
            & (pl.col("desc").str.contains("(?i)bagel").sum() > 0)
            & (pl.col("desc").str.contains("(?i)clean").sum() > 0)
        )
        .filter("bagel_coffee_clean")
        .join(customers, on="phone")
        .select(customers_df.collect_schema().names())
    ).collect()


# %%
products.pipe(the_contractor, orders_items, orders, customers)

# %%
_.pipe(answer)


# %% [markdown]
# ## 3. The Neighbor
#
# Sarah and the investigator were very impressed with your data skills, as you
# were able to figure out the phone number of the contractor. They called up the
# cleaning contractor straight away and asked about the rug.
#
# "Oh, yeah, I did some special projects for them a few years ago. I remember
# that rug unfortunately. I managed to clean one section, which revealed a giant
# spider that startled me whenever I tried to work on it.
#
# "I already had a fear of spiders before this, but this spider was so realistic
# that I had a hard time making any more progress. I kept expecting the cleaners
# would call for the rug, but they never did. I felt so bad about it, I couldn't
# face them, and of course they never gave me another project.
#
# "At last I couldn't deal with the rug taking up my whole bathtub, so I gave it
# to this guy who lived in my neighborhood. He said that he was naturally
# balanced because he was a Libra born in the year of the Goat, so maybe he was
# able to clean it.
#
# "I don't remember his name. Last time I saw him, he was leaving the subway and
# carrying a bag from Noah's. I swore I saw a spider on his hat."
#
# Can you find the phone number of the person that the contractor gave the rug
# to?


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
ZODIAC_LINK = "https://en.wikipedia.org/wiki/Astrological_sign"
zodiac = pl.from_pandas(pd.read_html(ZODIAC_LINK)[0])

zodiac


# %%
def zodiac_characteristics(
    zodiac_df: pl.LazyFrame = zodiac, zodiac_sign: ZodiacSign = ZodiacSign.Libra
) -> dict[str, list[str | int]]:
    """
    Extracts zodiac characteristics from a DataFrame based on the specified
    zodiac sign.

    Parameters
    ----------
    zodiac_df : pl.LazyFrame, optional
        The DataFrame containing zodiac information. By default, uses a
        predefined zodiac DataFrame.
    zodiac_sign : ZodiacSign, optional
        The zodiac sign for which characteristics are extracted. By default,
        uses ZodiacSign.Libra.

    Returns
    -------
    dict[str, list[str | int]]
        A dictionary containing zodiac characteristics:
        - 'dates': List of date strings.
        - 'month': List of month values.
        - 'days': List of day values.
    """
    return (
        zodiac_df.filter(pl.col("Sign") == zodiac_sign.value)
        .select(cs.contains("Sun"))
        .transpose()
        .rename({"column_0": "dates"})
        .with_columns(
            timestamp_fmt=pl.col("dates").map_elements(
                lambda _: datetime.strptime(f"{_} 1900", "%d %B %Y"),
                return_dtype=pl.Datetime,
            )
        )
        .with_columns(
            month=pl.col("timestamp_fmt").dt.month(),
            days=pl.col("timestamp_fmt").dt.day(),
        )
        .select(["dates", "month", "days"])
        .to_dict(as_series=False)
    )


# %%
zodiac_characteristics()


# %%
def chinese_sign_years(
    customers_df: pl.LazyFrame = customers,
    chinese_zodiac_animal: ChineseZodiac = ChineseZodiac.Goat,
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

    tables = [
        pl.from_pandas(table) for table in pd.read_html(chinese_zodiac_animal.value)
    ]

    for i, d in enumerate(tables):
        cols = list(d.columns)
        if "Start date" in cols:
            date_table_index = i

    return (
        tables[date_table_index]
        .select(pl.col("Start date").str.slice(-4).cast(pl.Int16))
        .join(
            customers.select(pl.col("birthdate").dt.year().cast(pl.Int16)).collect(),
            left_on="Start date",
            right_on="birthdate",
        )
        .unique()
        .rename({"Start date": "Chinese Sign Years"})
        .sort("Chinese Sign Years")
        .to_series()
        .to_list()
    )


# %%
chinese_sign_years()


# %%
def western_astrology_with_chinese_dates(
    western_astrology_sign: ZodiacSign = ZodiacSign.Libra,
    chinese_astrology_animal: ChineseZodiac = ChineseZodiac.Goat,
) -> pl.Series:
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
    animal_years: list[int] = chinese_sign_years(
        chinese_zodiac_animal=chinese_astrology_animal
    )

    months: list[int] = sign_dict["month"]
    start_month, end_month = months

    days: list[int] = sign_dict["days"]
    start_day, end_day = days

    return pl.concat(
        [
            pl.date_range(
                start=date(year=animal_year, month=start_month, day=start_day),
                end=date(year=animal_year, month=end_month, day=end_day),
                eager=True,
            )
            for animal_year in animal_years
        ]
    ).unique()


# %%
def the_neighbor(
    customers_df: pd.DataFrame = customers,
    western_astrology_sign: ZodiacSign = ZodiacSign.Libra,
    chinese_astrology_animal: ChineseZodiac = ChineseZodiac.Goat,
) -> pd.DataFrame:
    dates = western_astrology_with_chinese_dates(
        western_astrology_sign, chinese_astrology_animal
    )
    the_contractor_contact = the_contractor(
        products, orders_items, orders, customers_df
    )
    return (
        customers_df.filter(pl.col("birthdate").is_in(dates))
        .with_columns(
            zip_code=pl.col("citystatezip").str.slice(-5),
        )
        .filter(
            pl.col("zip_code").is_in(
                the_contractor_contact.select(pl.col("citystatezip").str.slice(-5)),
            )
        )
        .select(customers_df.collect_schema().names())
    ).collect()


# %%
the_neighbor(customers)

# %%
_.pipe(answer)


# %% [markdown]
# ## 4. The Early Bird
#
# The investigator called the phone number you found and left a message, and a
# man soon called back:
#
# "Wow, that was years ago! It was quite an elegant tapestry.
#
# "It took a lot of patience, but I did manage to get the dirt out of one
# section, which uncovered a superb owl. I put it up on my wall, and sometimes
# at night I swear I could hear the owl hooting.
#
# "A few weeks later my bike chain broke on the way home, and I needed to get it
# fixed before work the next day. Thankfully, this woman I met on Tinder came
# over at 5am with her bike chain repair kit and some pastries from Noah's.
# Apparently she liked to get up before dawn and claim the first pastries that
# came out of the oven.
#
# "I didn't have any money or I would've paid her for her trouble. She really
# liked the tapestry, though, so I wound up giving it to her.
#
# "I don't remember her name or anything else about her."
#
# Can you find the bicycle fixer's phone number?


# %%
def the_early_bird(
    products_df: pl.LazyFrame,
    orders_items_df: pl.LazyFrame,
    orders_df: pl.LazyFrame,
    customers_df: pl.LazyFrame,
) -> pl.DataFrame:
    return (
        products_df.filter(pl.col("sku").str.contains("BKY"))
        .join(orders_items_df, on="sku")
        .join(
            orders_df.filter(
                pl.col("ordered").dt.hour() < 5, pl.col("shipped").dt.hour() < 5
            ),
            on="orderid",
        )
        .group_by("customerid")
        .len()
        .top_k(1, by="len")
        .join(customers_df, on="customerid")
        .select(customers_df.collect_schema().names())
    ).collect()


# %%
the_early_bird(products, orders_items, orders, customers)

# %%
_.pipe(answer)


# %% [markdown]
# ## 5. The Cat Lady
#
# "Yes, I did have that tapestry for a little bit. I even cleaned a blotchy
# section that turned out to be a friendly koala.
#
# "But it was still really dirty, so when I was going through a Marie Kondo
# phase, I decided it wasn't sparking joy anymore.
#
# "I listed it on Freecycle, and a woman came to pick it up. She was wearing a
# 'Noah's Market' sweatshirt, and it was just covered in cat hair. When I
# suggested that a clowder of cats might ruin such a fine tapestry, she looked
# at me funny. She said "I only have ten or eleven cats, and anyway they are
# getting quite old now, so I doubt they'd care about some old rug."
#
# "It took her 20 minutes to stuff the tapestry into some plastic bags she
# brought because it was raining. I spent the evening cleaning my apartment."
#
# What's the phone number of the woman from Freecycle?


# %%
def the_cat_lady(
    products_df: pl.LazyFrame,
    orders_items_df: pl.LazyFrame,
    orders_df: pl.LazyFrame,
    customers_df: pl.LazyFrame,
) -> pl.DataFrame:
    return (
        products_df.filter(pl.col("desc").str.contains("(?i)senior cat"))
        .join(orders_items_df, on="sku")
        .join(orders_df, on="orderid")
        .group_by("customerid")
        .len()
        .top_k(1, by="len")
        .join(customers, on="customerid")
        .select(customers.collect_schema().names())
    ).collect()


# %%
the_cat_lady(products, orders_items, orders, customers)

# %%
_.pipe(answer)


# %% [markdown]
# ## 6. The Bargain Hunter
#
# "Why yes, I did have that rug for a little while in my living room! My cats
# can't see a thing but they sure chased after the squirrel on it like it was
# dancing in front of their noses.
#
# "It was a nice rug and they were surely going to ruin it, so I gave it to my
# cousin, who was moving into a new place that had wood floors.
#
# "She refused to buy a new rug for herself--she said they were way too
# expensive. She's always been very frugal, and she clips every coupon and shops
# every sale at Noah's Market. In fact I like to tease her that Noah actually
# loses money whenever she comes in the store.
#
# "I think she's been taking it too far lately though. Once the subway fare
# increased, she stopped coming to visit me. And she's really slow to respond to
# my texts. I hope she remembers to invite me to the family reunion next year."
#
# Can you find her cousin's phone number?
# %%
def the_bargain_hunter(
    customers_df: pl.LazyFrame = customers,
    products_df: pl.LazyFrame = products,
    orders_items_df: pl.LazyFrame = orders_items,
    orders_df: pl.LazyFrame = orders,
) -> pl.DataFrame:
    return (
        orders_items_df.join(products_df, on="sku")
        .filter(pl.col("unit_price") < pl.col("wholesale_cost"))
        .join(orders_df, on="orderid")
        .join(customers_df, on="customerid")
        .group_by("customerid")
        .len()
        .top_k(1, by="len")
        .join(customers_df, on="customerid")
        .select(customers_df.collect_schema().names())
    ).collect()


# %%
customers.pipe(the_bargain_hunter)

# %%
_.pipe(answer)


# %% [markdown]
# ## 7. The Meet Cute
#
# "Oh that tapestry, with the colorful toucan on it! I'll tell you what happened
# to it.
#
# "One day, I was at Noah's Market, and I was just about to leave when someone
# behind me said 'Miss! You dropped something!'
#
# "Well I turned around to see this cute guy holding an item I had bought. He
# said, 'I got the same thing!' We laughed about it and wound up swapping items
# because I wanted the color he got. We had a moment when our eyes met and my
# heart stopped for a second. I asked him to get some food with me and we spent
# the rest of the day together.
#
# "Before long I moved into his place, but the romance faded quickly, as he
# wasn't the prince I imagined. I left abruptly one night, forgetting the
# tapestry on his wall. But by then, it symbolized our love, and I wanted
# nothing more to do with it. For all I know, he still has it."
#
# Can you figure out her ex-boyfriend's phone number?


# %%
def color_agnostic_item_name(products_df: pl.LazyFrame = products) -> pl.DataFrame:
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
        products_df.filter(pl.col("desc").str.contains(r"\(\w+\)"))
        .with_columns(
            color=pl.col("desc")
            .str.split("(")
            .list.last()
            .str.replace_all(")", "", literal=True)
        )
        .with_columns(
            color_agnostic_desc=pl.col("desc").str.replace_all(r"\s(\(\w+\))", "")
        )
    )


# %%
def date_hour_mm(orders_df: pl.LazyFrame = orders) -> pl.LazyFrame:
    """
    Extracts and formats the date and hour from the 'ordered' column in a
    DataFrame of order information.

    Parameters
    ----------
    orders_df : pl.LazyFrame, optional
        DataFrame containing order information, by default orders.

    Returns
    -------
    pl.LazyFrame
        DataFrame with an additional 'date_hour' column representing the
        formatted date and hour.

    Notes
    -----
    This function extracts the date and hour information from the 'ordered'
    column and adds a new column 'date_hour' to the DataFrame. The 'date_hour'
    column is formatted as 'MM/DD/YYYY HH:MM' to represent the month, day,
    year, and hour of each order.
    """
    return orders_df.with_columns(
        date_hour=pl.col("ordered").dt.strftime("%m/%d/%Y %H:%M")
    )


# %%
def filter_in_store_orders(orders_df: pl.LazyFrame = orders) -> pl.LazyFrame:
    """
    Filters out in-store orders from a DataFrame of order information.

    Parameters
    ----------
    orders_df : pl.LazyFrame, optional
        DataFrame containing order information, by default orders.

    Returns
    -------
    pl.LazyFrame
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
    return orders_df.filter(pl.col("ordered") == pl.col("shipped"))


# %%
def bargain_hunter_color_items(
    products_df: pl.LazyFrame = products,
    orders_items_df: pl.LazyFrame = orders_items,
    orders_df: pl.LazyFrame = orders,
) -> pl.DataFrame:
    return (
        products_df.pipe(color_agnostic_item_name)
        .join(orders_items_df, on="sku")
        .join(orders_df, on="orderid")
        .filter(
            pl.col("customerid")
            == customers.pipe(
                the_bargain_hunter, products, orders_items, orders
            ).select("customerid")
        )
        .pipe(filter_in_store_orders)
        .pipe(date_hour_mm)
    )


# %%
def the_order_of_the_meet(
    orders_df: pl.LazyFrame = orders,
    orders_items_df: pl.LazyFrame = orders_items,
    products_df: pl.LazyFrame = products,
) -> pl.DataFrame:
    bargain_hunter = bargain_hunter_color_items()

    return (
        orders_df.pipe(filter_in_store_orders)
        .join(orders_items_df, on="orderid")
        .pipe(date_hour_mm)
        .join(products_df.pipe(color_agnostic_item_name), on="sku")
        .join(
            bargain_hunter.select(["color_agnostic_desc", "date_hour"]),
            on=["color_agnostic_desc", "date_hour"],
        )
        .filter(
            ~pl.col("customerid").is_in(
                bargain_hunter.select("customerid").collect().to_series()
            )
        )
        .select(bargain_hunter.collect_schema().names())
        .collect()
        .pipe(
            lambda cute_guy: pl.concat(
                [
                    bargain_hunter.collect().filter(
                        pl.col("color_agnostic_desc").is_in(
                            cute_guy.select("color_agnostic_desc").to_series()
                        ),
                        pl.col("date_hour").is_in(
                            cute_guy.select("date_hour").to_series()
                        ),
                    ),
                    cute_guy,
                ]
            )
        )
    )


# %%
the_order_of_the_meet()


# %%
def the_meet_cute(customers_df: pl.LazyFrame = customers) -> pl.DataFrame:
    the_meet = the_order_of_the_meet()
    the_couple_customer_ids = the_meet.select("customerid").to_series().to_list()
    the_bargain_hunter_customer_id = (
        customers_df.pipe(the_bargain_hunter).select("customerid").to_series().to_list()
    )
    the_meet_cute_id = set(the_couple_customer_ids) - set(
        the_bargain_hunter_customer_id
    )
    return customers_df.filter(
        pl.col("customerid").is_in(list(the_meet_cute_id))
    ).collect()


# %%
customers.pipe(the_meet_cute)

# %%
_.pipe(answer)


# %% [markdown]
# ## 8. The Collector
#
# "Oh that damned woman! She moved in, clogged my bathtub, left her coupons all
# over the kitchen, and then just vanished one night without leaving so much as
# a note.
#
# Except she did leave behind that nasty carpet. I spent months cleaning one
# corner, only to discover a snake hiding in the branches! I knew then that she
# was never coming back, and I had to get it out of my sight.
#
# "Well, I don't have any storage here, and it didn't seem right to sell it, so
# I gave it to my sister. She wound up getting a newer and more expensive
# carpet, so she gave it to an acquaintance of hers who collects all sorts of
# junk. Apparently he owns an entire set of Noah's collectibles! He probably
# still has the carpet, even.
#
# "My sister is away for the holidays, but I can have her call you in a few
# weeks."
#
# The family dinner is tonight! Can you find the collector's phone number in
# time?


# %%
def the_collector(
    customers_df: pl.LazyFrame = customers,
    products_df: pl.LazyFrame = products,
    orders_items_df: pl.LazyFrame = orders_items,
    orders_df: pl.LazyFrame = orders,
) -> pl.DataFrame:
    return (
        products_df.filter(pl.col("desc").str.contains("Noah"))
        .join(orders_items_df, on="sku")
        .join(orders_df, on="orderid")
        .join(customers_df, on="customerid")
        .group_by("customerid")
        .len()
        .top_k(1, by="len")
        .join(customers_df, on="customerid")
        .select(customers_df.collect_schema().names())
    ).collect()


# %%
customers.pipe(the_collector)

# %%
_.pipe(answer)

# %% [markdown]
# ## 9. Epilogue
#
# "Oh yes, that magnificant Persian carpet! An absolute masterpiece, with a
# variety of interesting animals congregating around a Tree of Life. As a
# collector, I couldn't believe when it fell into my lap.
#
# "A friend of mine had taken it off her brother's hands, and she didn't know
# what to do with it. I saw her one day, and she was about to put an old rug out
# at the curb. It looked like it had been through a lot, but it was remarkably
# not that dirty. It still took quite a bit of effort and no small amount of rug
# cleaner, but ultimately I managed to get the last bits of grime out of it.
#
# "I actually live right down the street from Noah's Market--I'm a huge fan and
# I shop there all the time! I even have a one-of-a-kind scale model of Noah's
# Ark that makes a complete set of Noah's collectibles.
#
# "I would love for Noah to have his rug once again to enjoy."

# %% [markdown]
# ![](tapestry.gif)
