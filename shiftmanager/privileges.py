"""
Functions for reproducing GRANT statements from queried access privileges.
These assume privileges are input in the format of psql's \z or \dp commands,
which can be obtained by ::

    pg_catalog.array_to_string(relacl, '\n') FROM pg_catalog.pg_class

Details from http://www.postgresql.org/docs/7.4/static/sql-grant.html

The entries shown by \z are interpreted thus ::

              =xxxx -- privileges granted to PUBLIC
         uname=xxxx -- privileges granted to a user
   group gname=xxxx -- privileges granted to a group

                  r -- SELECT ("read")
                  w -- UPDATE ("write")
                  a -- INSERT ("append")
                  d -- DELETE
                  R -- RULE
                  x -- REFERENCES
                  t -- TRIGGER
                  X -- EXECUTE
                  U -- USAGE
                  C -- CREATE
                  T -- TEMPORARY
            arwdRxt -- ALL PRIVILEGES (for tables)
                  * -- grant option for preceding privilege

              /yyyy -- user who granted this privilege
"""


import re


RELACL_CHARS_TO_WORDS = {
    'r': 'SELECT',
    'w': 'UPDATE',
    'a': 'INSERT',
    'd': 'DELETE',
    'R': 'RULE',
    'x': 'REFERENCES',
    't': 'TRIGGER',
    'X': 'EXECUTE',
    'U': 'USAGE',
    'C': 'CREATE',
    'T': 'TEMPORARY',
}

WITH_GRANT_OPTION_RE = re.compile(r'[arwdRxtXUCT]\*')


def grants_from_privileges(privileges, relation):
    """
    >>> grants_from_privileges('=r/ops\\nimporter=arwdRxt/ops', 'foo')
    ['GRANT SELECT ON foo TO PUBLIC', 'GRANT ALL ON foo TO importer']
    """
    grants = []
    if privileges:
        for entry in privileges.split('\n'):
            grants += grants_from_entry(entry, relation)
    return grants


def grants_from_entry(entry, relation):
    """
    >>> grants_from_entry('=r/ops', 'foo')
    ['GRANT SELECT ON foo TO PUBLIC']

    >>> grants_from_entry('importer=arwdRxt/ops', 'bar')
    ['GRANT ALL ON bar TO importer']

    >>> grants = grants_from_entry('importer=ar*wd*/ops', 'baz')
    >>> print(grants[0])
    GRANT INSERT, UPDATE ON baz TO importer
    >>> print(grants[1])
    GRANT SELECT, DELETE ON baz TO importer WITH GRANT OPTION

    >>> grants_from_entry('group finance=r/importer', 'foo')
    ['GRANT SELECT ON foo TO GROUP finance']
    """
    grantee, _, rest = entry.partition('=')
    grantee = grantee.replace('group', 'GROUP') or 'PUBLIC'
    chars, _, grantor = rest.partition('/')
    grants = []
    words, words_with_grant_option = words_from_relacl_chars(chars)
    if words:
        grant = "GRANT %s ON %s TO %s" % (', '.join(words), relation, grantee)
        grants.append(grant)
    if words_with_grant_option:
        grant = ("GRANT %s ON %s TO %s WITH GRANT OPTION" %
                 (', '.join(words_with_grant_option), relation, grantee))
        grants.append(grant)
    return grants


def words_from_relacl_chars(chars):
    """
    >>> words_from_relacl_chars('arwdRxt')
    (['ALL'], [])

    >>> words_from_relacl_chars('r')
    (['SELECT'], [])

    >>> words_from_relacl_chars('r*')
    ([], ['SELECT'])

    >>> words_from_relacl_chars('ar*wd*Rx')
    (['INSERT', 'UPDATE', 'RULE', 'REFERENCES'], ['SELECT', 'DELETE'])
    """
    words, words_with_grant_option = [], []
    if chars == 'arwdRxt':
        words.append('ALL')
        return (words, words_with_grant_option)
    for match in WITH_GRANT_OPTION_RE.findall(chars):
        char, asterisk = match
        word = RELACL_CHARS_TO_WORDS[char]
        words_with_grant_option.append(word)
        chars = chars.replace(match, '')
    for char in chars:
        word = RELACL_CHARS_TO_WORDS[char]
        words.append(word)
    return (words, words_with_grant_option)
