blacklist = set()

def add_to_blacklist(jti):
    blacklist.add(jti)

def is_token_revoked(jti):
    return jti in blacklist
