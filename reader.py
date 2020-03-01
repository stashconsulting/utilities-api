import re


def read_document(doc: str):
    with open(doc, "r") as fi:
        return ''.join([line for line in fi])


def extract_values(text: str):

    data = re.findall(
        r"RD\$(.+?)\/",
        text
    )

    if (_from := re.search(r"cierre\sdel\s(.*?),", text)):
        data.append(_from.group())

    if (until := re.search(r"hasta\sel\s.*?(\d.+?)\.",text)):
        data.append(until.group())

    return data


def main():
    print(extract_values(read_document('tasaus_mc.txt')))


if __name__ == "__main__":
    main()
