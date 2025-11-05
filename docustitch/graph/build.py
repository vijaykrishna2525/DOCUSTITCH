def build_graph(sections, explicit_edges, implicit_edges=None):
    return {"nodes":[{"sec_id":s.get("sec_id","")} for s in sections], "edges": []}
