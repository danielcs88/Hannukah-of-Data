# %%
# pylint: disable=wrong-import-position missing-module-docstring invalid-name
import os

# %% [markdown]
# # Hanukkah of Data/5783
#
# ## Noah’s Market
#
# Your granduncle Noah owns “Noah’s Market”, an old-fashioned mom-and-pop
# everything store in NYC. In recent years it’s become quite an operation, with
# over a thousand products and customers all over the U.S. They still have the
# same Manhattan storefront, and they’re still running on the same database your
# cousin Alex set up at the start of 2017.
#
# You were in Noah’s buying some bagels, when your Aunt Sarah pulled you aside
# and asked if you could help her with something.
#
# “You know how Noah’s been talking recently about that rug we used to have?”
#
# She looked over at Noah, who was talking to a customer: “Such a beautiful rug,
# with the most intricate design! I miss having it in my living room. It has
# this vibrant beehive buzzing along the edge…”
#
# Sarah said, “Noah gave it to me a few years ago for safekeeping. It was so old
# and filthy, that I had to send it to the cleaners. Now that Noah’s retiring
# and I’ll be taking over the store, he wants that old rug back, so he can put
# it in his new den.
#
# “The problem is, after I sent it to the cleaners, I forgot about it. I
# apparently never went to pick it up. I combed the apartment yesterday and I
# finally found this claim ticket. ‘All items must be picked up within 90 days.’
# it says on it.
#
# “Well I took it back to the cleaners, but they didn’t have the rug. They did
# have the other half of the ticket, though! The ticket had ‘2017 spec JD’
# written on it. The clerk was super busy and said they didn’t have time for an
# ancient claim ticket.
#
# “I’d really like to find this rug, before Noah comes over for our family
# dinner on the last day of Hanukkah. I would normally ask Alex to help me with
# this, but Alex said they wouldn’t be able to get to it until after the new
# year. I think it’s because Alex is spending all day working on those Advent of
# Code problems.
#
# “Do you think you could help me track down the rug?”
#
# She hands you a [USB drive](https://hanukkah.bluebird.sh/5783/data) labeled
# “Noah’s Market Database Backup”.
#
# “Alex set up the backups to be password-protected. I can never remember the
# password itself, but it’s just the year in the Hebrew calendar when Alex set
# up the database.”
#
# What’s the password to open the .zip files on the USB drive?

# %% [markdown]
#
# > Google'd 2017 to Hebrew Calendar
# > Also relevant is the the fact that on the banner our current year, 2022 is
# > featured as 5783
# >
# > ![image](https://bit.ly/3BX9dBS)

# %%
password = 5783 - 6
print(password)

# %%
os.system(f"unzip -P {password} noahs-csv")

# %% [markdown]
# # Puzzle 1
#
# Sarah brought over one of the cashiers. She said, “Joe here says that one of
# our customers is a skilled private investigator.”
#
# Joe nodded, “They came in awhile ago and showed me their business card, and
# that’s what it said. Skilled Private Investigator. And their phone number was
# their last name spelled out. I didn’t know what that meant, but apparently
# before there were smartphones, people had to remember phone numbers or write
# them down. If you wanted a phone number that was easy-to-remember, you could
# get a number that spelled something using the letters printed on the phone
# buttons: like 2 has “ABC”, and 3 “DEF”, etc. And I guess this person had done
# that, so if you dialed the numbers corresponding to the letters in their name,
# it would call their phone number!
#
# “I thought that was pretty cool. But I don’t remember their name, or anything
# else about them for that matter. I couldn’t even tell you if they were male or
# female.”
#
# Sarah said, “This person seems like they are clever and skilled at
# investigation. I’d like to hire them to help me find Noah’s rug before the
# Hanukkah dinner. I don’t know how to contact them, but apparently they shop
# here at Noah’s Market.”
#
# She nodded at the [USB drive](https://hanukkah.bluebird.sh/5783/data) in your
# hand.
#
# “Can you find this private investigator’s phone number?”

# %% [markdown]
# ### Rationale
#
# How long are phone numbers? 10 digits.

# %%
for _ in "Skilled Private Investigator".split():
    print(len(_))

# %% [markdown]
# So much for that. Time to use Pandas

# %%
from datetime import datetime
from typing import Union

import numpy as np
import pandas as pd
import pyperclip
from IPython.display import display

customers = pd.read_csv(
    "noahs-csv/noahs-customers.zip", parse_dates=["birthdate"]
).drop_duplicates(subset=["customerid"])


# %% [markdown]
# ### Idea
#
# Filter out for names that together are 10 characters.

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
def set_customer_id_index(df):
    return df.set_index("customerid")

# %%
puzzle_1 = (
    customers.loc[~customers["name"].str.endswith(("II", "III", "IV", "Jr."))]
    .copy()
    .assign(
        last_name=customers["name"].str.split().str[-1].str.lower(),
        length=lambda df: df["last_name"].str.len(),
    )
    .query("length == 10")
    .assign(
        name_char=lambda df: df["last_name"].apply(list),
        phone_num=lambda df: df["name_char"].apply(
            lambda _: "".join([str(translate_char_to_phone_num(x)) for x in _])
        ),
        test=lambda df: df["phone_num"].str.slice(stop=3)
        + "-"
        + df["phone_num"].str.slice(start=3, stop=6)
        + "-"
        + df["phone_num"].str.slice(start=6),
    )
    .query("phone == test")
    .filter(customers.columns)
)

# %%
display(puzzle_1.pipe(set_customer_id_index))


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
print(answer(puzzle_1))

# %% [markdown]
# ## Puzzle 2
#
# With your help, Sarah was able to call the private investigator that
# afternoon, and brought them up to speed. The investigator went to the cleaners
# directly to see if they could get any more information about the unclaimed
# rug.
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
# said ‘2017 spec JD’. ‘2017’ is the year the item was brought in, and ‘JD’ is
# the initials of the contractor.
#
# “But they stopped outsourcing a few years ago, and don’t have contact
# information for any of these workers anymore.”
#
# Sarah first seemed hopeless, and then looked at the [USB
# drive](https://hanukkah.bluebird.sh/5783/data) you had just put back in her
# hand. She said, “I know it’s a long shot, but is there any chance you could
# find their phone number?”

# %%
orders = pd.read_csv("noahs-csv/noahs-orders.zip", parse_dates=["ordered", "shipped"])
orders_items = pd.read_csv("noahs-csv/noahs-orders_items.zip")
products = pd.read_csv("noahs-csv/noahs-products.zip")

# %%
puzzle_2 = (
    customers.replace([" II", " III", " IV", " Jr."], "")
    .assign(
        initials=customers["name"].str.split(" ").str[0].str[0]
        + customers["name"].str.split(" ").str[-1].str[0]
    )
    .merge(orders.set_index("ordered").loc["2017"], on="customerid")
    .query('initials == "JD"')
    .merge(orders_items, on="orderid")
    .merge(
        products.loc[
            products["desc"].str.lower().str.contains("coffee, drip|bagel")
        ].copy(),
        on="sku",
    )
    .filter(customers.columns)
).drop_duplicates()

# %%
display(puzzle_2.pipe(set_customer_id_index))

# %%
print(answer(puzzle_2))

# %% [markdown]
# ## Puzzle 3
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
# assertive because he was a Aries born in the year of the Dog, so maybe he was
# able to clean it.
#
# “I don’t remember his name. Last time I saw him, he was leaving the subway and
# carrying a bag from Noah’s. I swore I saw a spider on his hat.”
#
# Can you find the phone number of the person that the contractor gave the rug
# to?

# %% [markdown]
# ### Clues
#
# - Aries
# - Dog Chinese Zodiac
# - Spider on Hat?
# - It's a Guy

# %%
ZODIAC_LINK = "https://en.wikipedia.org/wiki/Astrological_sign"
DOG_ZODIAC_LINK = "https://en.wikipedia.org/wiki/Dog_(zodiac)"

# %%
zodiac = pd.read_html(ZODIAC_LINK)[0]

# %%
display(zodiac)


# %%
def month_day(date: str) -> datetime:
    r"""
    Convert "Month Day" string to datetime

    e.g., December 11 -> datetime.datetime(1900, 12, 11, 0, 0)


    Parameters
    ----------
    date : str

    Returns
    -------
    datetime.datetime
    """
    return datetime.strptime(date, "%B %d")


# %%
aries = (
    (
        zodiac.query("Sign == 'Aries'")["Approximate Sun Sign Dates"]
        .str.split(" – ", expand=True)
        .T
    )
    .rename(columns={0: "text"})
    .assign(
        month=lambda df: df["text"].apply(month_day).apply(lambda _: _.month),
        day=lambda df: df["text"].apply(month_day).apply(lambda _: _.day),
    )
    .copy()
    .to_dict("list")
)

# %%
display(aries)

# %%
dog = set(pd.to_numeric(pd.read_html(DOG_ZODIAC_LINK)[2]["Start date"].str[-4:])) & set(
    customers["birthdate"].dt.year
)

# %%
print(dog)

# %%
aries_dog = [
    pd.Period.to_timestamp(date)
    for date in np.concatenate(
        [
            pd.period_range(
                start=pd.Timestamp(f'{aries["month"][0]}/{aries["day"][0]}/{y}'),
                end=pd.Timestamp(f'{aries["month"][1]}/{aries["day"][1]}/{y}'),
            )
            for y in sorted(dog)
        ],
        axis=0,
    )
]

# %%
display(aries_dog)

# %%
puzzle_3 = (
    customers.loc[customers["birthdate"].isin(aries_dog)]
    .copy()
    .assign(
        zip=lambda df: df["citystatezip"].str[-5:],
        neighbor=lambda df: df["zip"] == puzzle_2["citystatezip"].str[-5:].iloc[0],
    )
    .query("neighbor == True")
    .filter(customers.columns)
)

# %%
display(puzzle_3.pipe(set_customer_id_index))

# %%
print(answer(puzzle_3))

# %% [markdown]
# ## Puzzle 4
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
puzzle_4 = customers.loc[
    customers["customerid"].isin(
        orders.loc[(orders["ordered"].dt.hour < 5) & (orders["shipped"].dt.hour < 5)]
        .merge(orders_items, on="orderid")
        .loc[lambda df: df["sku"].str[:3] == "BKY"]
        .query("qty > 1")["customerid"]
        .mode()
    )
]

# %%
display(puzzle_4.pipe(set_customer_id_index))

# %%
print(answer(puzzle_4))

# %% [markdown]
# ## Puzzle 5
#
# “Yes, I did have that tapestry for a little bit. I even cleaned a blotchy
# section that turned out to be a friendly koala.
#
# “But it was still really dirty, so when I was going through a Marie Kondo
# phase, I decided it wasn’t sparking joy anymore.
#
# “I listed it on Freecycle, and a woman in Queens Village came to pick it up.
# She was wearing a ‘Noah’s Market’ sweatshirt, and it was just covered in cat
# hair. When I suggested that a clowder of cats might ruin such a fine tapestry,
# she looked at me funny and said she only had ten or eleven cats and they were
# getting quite old and had cataracts now so they probably wouldn’t notice some
# old rug anyway.
#
# “It took her 20 minutes to stuff the tapestry into some plastic bags she
# brought because it was raining. I spent the evening cleaning my apartment.”
#
# What’s the phone number of the woman from Freecycle?

# %%
puzzle_5 = customers.loc[
    customers["customerid"]
    == (
        customers.loc[
            customers["citystatezip"].str.split(",").str[0] == "Queens Village"
        ]
        .merge(orders, on="customerid")
        .merge(orders_items, on="orderid")
        .merge(products, on="sku")
        .loc[
            lambda df: df["sku"].isin(
                products.loc[products["desc"].str.lower().str.contains("cat")][
                    "sku"
                ].unique()
            )
        ]["customerid"]
        .mode()
        .iloc[0]
    )
]

# %%
display(puzzle_5.pipe(set_customer_id_index))

# %%
print(answer(puzzle_5))

# %% [markdown]
# ## Puzzle 6
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
puzzle_6 = customers.loc[
    customers["customerid"]
    == orders_items.groupby(["sku", "orderid"], as_index=False)["unit_price"]
    .min()
    .merge(products, on="sku")
    .query("unit_price <= wholesale_cost")
    .merge(orders, on="orderid")
    .merge(customers, on="customerid")["customerid"]
    .mode()
    .iloc[0]
].copy()

# %%
display(puzzle_6.pipe(set_customer_id_index))

# %%
print(answer(puzzle_6))


# %% [markdown]
# ## Puzzle 7
#
# “Oh that tapestry, with the colorful toucan on it! I’ll tell you what happened
# to it.
#
# “One day, I was at Noah’s Market, and I was just about to leave when someone
# behind me said ‘Miss! You dropped something!’
#
# “Well I turned around and sure enough this cute guy was holding something I
# had bought. He said ‘I got almost exactly the same thing!’ We laughed about it
# and wound up swapping items because he had wanted the color I got. We had a
# moment when our eyes met and my heart stopped for a second. I asked him to get
# some food with me and we spent the rest of the day together.
#
# “Before long I moved into his place. It didn’t last long though, as I soon
# discovered this man was not the gentleman I thought he was. I moved out only a
# few months later, late at night and in quite a hurry.
#
# “I realized the next day that I’d left that tapestry hanging on his wall. But
# the tapestry had come to represent our relationship, and I wanted nothing more
# to do with him, so I let it go. For all I know, he still has it.”
#
# Can you figure out her ex-boyfriend’s phone number?

# %% [markdown]
# ### Rationale
#
# - Jerseys have colors, it could be matching two orders on the same day with
#   jerseys. Query through all products that the word color.
#
# - ~~Same addresses?~~: She didn't change her address at Noah's
#
#   > ```python
#   > customers.loc[customers['address'].isin(puzzle_6['address'])]
#   > ```
#
# - Orders that match dates with the previous person: **Emily Randolph**
#
# - Find orders that have ```SKU.str[:3] == 'COL'``` and that their `ordered` and
#   `shipped` is within an arbitrary amount, could try `[10, 20, 30]`

# %%
def has_color(item: str) -> str:
    r"""
    Split item in to use on `products['desc']`, checks if item has a
    parenthesis. If so, strip first and last characters.

    Parameters
    ----------
    item : str

    Returns
    -------
    str

    """
    possible_color = item.split(" ")[-1]

    match possible_color[-1]:
        case ")":
            return possible_color[1:-1]


# %%
def date_hour(df: pd.DataFrame) -> pd.DataFrame:
    r"""
    Filters out orders that were bought in-store for orders

    Parameters
    ----------
    df : pd.DataFrame


    Returns
    -------
    pd.DataFrame
    """

    return df.assign(date_hour=lambda df: df["ordered"].dt.strftime("%m/%d/%Y %H"))
    # return df.assign(date=lambda df: df["ordered"].dt.date)


# %%
def filter_in_store_orders(df: pd.DataFrame) -> pd.DataFrame:
    r"""
    Filters out orders that were bought in-store for orders

    Parameters
    ----------
    df : pd.DataFrame


    Returns
    -------
    pd.DataFrame
    """
    return df.loc[df["ordered"] == df["shipped"]]


# %%
def color_agnostic_item_name(df: pd.DataFrame()) -> pd.DataFrame:
    r"""
    Generates a column for items that is color agnostic.

    e.g., Manual Mixer (orange) -> Manual Mixer

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """
    return df.assign(desc_color_agnostic=lambda df: df["desc"].str.split(r" \(").str[0])


# %%
colors = (
    products.loc[products["sku"].str[:3] == "COL"]
    .assign(color=lambda df: df["desc"].apply(has_color))
    .query("wholesale_cost < 100")["color"]
    .unique()
)

# %% [markdown]
# Here I queried for products that are over \$100 because one of these results
# isn't a color

# %%
print(colors)

# %%
colored_products = (
    products.loc[products["desc"].str.contains("|".join(colors))]
    .copy()
    .pipe(color_agnostic_item_name)
)

# %%
display(colored_products)

# %%
emily_in_color = (
    (
        orders_items.merge(colored_products, on="sku")
        .merge(
            orders.loc[orders["customerid"].isin(puzzle_6["customerid"])],
            on="orderid",
        )
        .merge(customers, on="customerid")
        .copy()
    )
    .pipe(filter_in_store_orders)
    .pipe(date_hour)
)

# %%
display(emily_in_color)

# %%
puzzle_7 = customers.loc[
    customers["customerid"]
    == (
        orders.pipe(filter_in_store_orders)
        .merge(orders_items.merge(colored_products, on="sku"), on="orderid")
        .pipe(date_hour)
        .loc[
            lambda df: (df["date_hour"].isin(emily_in_color["date_hour"].unique()))
            & (df["desc_color_agnostic"].isin(emily_in_color["desc_color_agnostic"]))
        ]
        .groupby("date_hour")["customerid"]
        .sum()
        - puzzle_6["customerid"].iloc[0]
    )
    .to_frame()
    .loc[lambda df: df["customerid"] != puzzle_6["customerid"].iloc[0]]
    .squeeze()
].copy()

# %%
display(puzzle_7.pipe(set_customer_id_index))

# %%
print(answer(puzzle_7))

# %% [markdown]
# ## Puzzle 8
#
# “Oh that damned woman! She moved in, clogged my bathtub, spilled oatmeal all
# over the kitchen, and then just vanished one night without leaving so much as
# a note. Well except she did leave behind that tapestry. We spent much of our
# time together cleaning one filthy area, only to reveal a snake hiding in the
# branches!
#
# “I left it on my wall hoping she would come back for it, but eventually I
# accepted that I had to move on.
#
# “I don’t have any storage here, and it didn’t seem right to sell it, so I gave
# it to my sister who lives in Manhattan. She wound up getting a newer and more
# expensive rug, so she gave it to an acquaintance of hers who collects all
# sorts of junk. Apparently he owns an entire set of Noah’s collectibles! He
# probably still has the rug, even.
#
# “My sister is away for the holidays, but I can have her call you in a few
# weeks.”
#
# The family dinner is tonight! Can you find the collector’s phone number in
# time?

# %%
puzzle_7 = customers.loc[
    customers["customerid"]
    == (
        orders.merge(orders_items, on="orderid")
        .set_index("customerid")
        .loc[
            lambda df: orders.merge(orders_items, on="orderid")["customerid"]
            .value_counts()
            .index
        ]
        .reset_index()["index"]
        .iloc[0]
    )
]

# %%
display(puzzle_7.pipe(set_customer_id_index))

# %%
print(answer(puzzle_7))

# %% [markdown]
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

# %% [markdown]
# ![HoD_2022](https://bit.ly/3YRdAZ2)
