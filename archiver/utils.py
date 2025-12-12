def parse_pipe_array(request, name):
    v = request.query_params.get(name)
    if not v:
        return None
    return [x for x in v.split("|") if x != ""]