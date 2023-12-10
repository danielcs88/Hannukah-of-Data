# %%
# ruff: noqa: E402
# pylint: disable=wrong-import-position missing-module-docstring invalid-name missing-function-docstring
import os

# %% [markdown]
# # Hanukkah of Data/5784
#
# ## Noah’s Market
#
# Welcome to “Noah’s Market”, a bustling mom-and-pop everything store in
# Manhattan. In recent years it’s become quite an operation, but they’re still
# running on the same database your cousin Alex set up at the start of 2017.
#
# This morning, while waiting for your breakfast bagel, your Aunt Sarah pulled
# you aside in a hustle.
#
# “You know how Noah’s been talking recently about that rug we used to have?”
#
# She looked over at Noah, who was talking to a customer: “Such a beautiful
# carpet, with the most intricate design! I miss having it in my den. It has
# this vibrant beehive buzzing in the corner…”
#
# Sarah explained, “Noah entrusted me with that rug when he was remodeling his
# den a few years ago. It was so old and filthy, that I sent it to the cleaners,
# but then I completely forgot about it. Now, with Noah retiring and me taking
# over the store, he wants it back. So yesterday I freaked out and combed my
# apartment, and I finally found a claim ticket saying, ‘All items must be
# picked up within 90 days.’ At the cleaners, they didn’t have the rug, just the
# other half of the ticket.”
#
# Sarah added, “I need to find that rug before Noah comes over on the last night
# of Hanukkah. I have an idea but I need some help, and Alex will be busy for
# weeks doing those Advent of Code challenges.
#
# “Do you think you could help me track down the rug?”
#
# She hands you a [USB drive](https://hanukkah.bluebird.sh/5784/data) labeled
# “Noah’s Market Database Backup”.
#
# “Alex set up the backups to be password-protected. I can never remember the
# password itself, but it’s just the year in the Hebrew calendar when Alex set
# up the database.”
#
# What’s the password to open the .zip files on the USB drive?

# %%
password = 5783 - 6
print(password)

# %%
os.system(f"unzip -P {password} noahs-csv")

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
# She nodded at the [USB drive](https://hanukkah.bluebird.sh/5784/data) in your
# hand.
#
# “Can you find this investigator’s phone number?”

# %%
from enum import Enum

import numpy as np
import pandas as pd
import pyperclip
from IPython.display import display

# %%
customers = pd.read_csv("5784/noahs-customers.csv.zip", parse_dates=["birthdate"])
orders = pd.read_csv("5784/noahs-orders.csv.zip", parse_dates=["ordered", "shipped"])
orders_items = pd.read_csv("5784/noahs-orders_items.csv.zip")
products = pd.read_csv("5784/noahs-products.csv.zip")

# %%
os.system("rm -rf 5784")


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
print(customers.pipe(one_the_investigator).pipe(answer))

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
# said ‘2017 JP’. ‘2017’ is the year the item was brought in, and ‘JP’ is the
# initials of the contractor.
#
# “But they stopped outsourcing a few years ago, and don’t have contact
# information for any of these workers anymore.”
#
# Sarah first seemed hopeless, and then glanced at the [USB
# drive](https://hanukkah.bluebird.sh/5784/data) you had just put back in her
# hand. She said, “I know it’s a long shot, but is there any chance you could
# find their phone number?”


# %%
def two_the_contractor(
    customers_df: pd.DataFrame = customers,
    orders_df: pd.DataFrame = orders,
    orders_items_df: pd.DataFrame = orders_items,
    products_df: pd.DataFrame = products,
) -> pd.DataFrame:
    return (
        (
            customers_df.replace([" II", " III", " IV", " Jr."], "")
            .assign(
                initials=(
                    customers_df["name"].str.split(" ").str[0].str[0]
                    + customers_df["name"].str.split(" ").str[-1].str[0]
                )
            )
            .merge(orders_df.set_index("ordered").loc["2017"], on="customerid")
            .query('initials == "JP"')
            .merge(orders_items_df, on="orderid")
            .merge(
                products_df.loc[
                    products["desc"].str.lower().str.contains("coffee, drip|bagel")
                ],
                on="sku",
            )
            .filter(customers.columns)
        )
        .drop_duplicates()
        .set_index("customerid")
    )


# %%
two_the_contractor().pipe(display)

# %%
two_the_contractor().pipe(answer)

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
# intuitive because he was a Cancer born in the year of the Rabbit, so maybe he
# was able to clean it.
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
display(zodiac)


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
    df: pd.DataFrame = zodiac, zodiac_sign: ZodiacSign = ZodiacSign.Cancer
) -> dict[str, list[str | int]]:
    """
    Extracts zodiac characteristics from a DataFrame based on the specified
    zodiac sign.

    Parameters
    ----------
    df : pd.DataFrame, optional
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
        (df.loc[df["Sign"] == zodiac_sign.value].filter(like="Sun", axis=1).T)
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
    df: pd.DataFrame = customers,
    chinese_zodiac_animal: ChineseZodiac = ChineseZodiac.Rabbit,
) -> set[int]:
    """
    Retrieves the birth years of customers belonging to a specific Chinese
    zodiac sign.

    Parameters
    ----------
    df : pd.DataFrame, optional
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
    return set(
        pd.to_numeric(
            pd.read_html(chinese_zodiac_animal.value)[2]["Start date"].str[-4:]
        )
    ) & set(df["birthdate"].dt.year)


# %%
chinese_sign_years(customers, ChineseZodiac.Dragon)


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
pd.Series(
    western_astrology_with_chinese_dates(ZodiacSign.Sagittarius, ChineseZodiac.Dragon)
)


# %%
def three_the_neighbor(
    df: pd.DataFrame = customers,
    western_astrology_sign: ZodiacSign = ZodiacSign.Cancer,
    chinese_astrology_animal: ChineseZodiac = ChineseZodiac.Rabbit,
) -> pd.DataFrame:
    dates = western_astrology_with_chinese_dates(
        western_astrology_sign, chinese_astrology_animal
    )
    the_contractor = two_the_contractor()
    return (
        df.loc[df["birthdate"].isin(dates)]
        .assign(
            zip_code=lambda d: d["citystatezip"].str[-5:],
            neighbor=lambda d: d["zip_code"]
            == the_contractor["citystatezip"].str[-5:].iloc[0],
        )
        .loc[lambda d: d["neighbor"]]
        .filter(df.columns)
    )


# %%
customers.pipe(three_the_neighbor).pipe(display)

# %%
customers.pipe(three_the_neighbor).pipe(answer)


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
earlybird_customer_id()


# %%
def four_the_early_bird(customers_df: pd.DataFrame = customers) -> pd.DataFrame:
    earlybird_customer: int = earlybird_customer_id()
    return customers_df.loc[customers_df["customerid"] == earlybird_customer]


# %%
customers.pipe(four_the_early_bird).pipe(display)

# %%
customers.pipe(four_the_early_bird).pipe(answer)
