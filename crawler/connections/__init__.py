from crawler.connections.manager import BasicConnectionManager, VPNConnectionManager

if __name__ == "__main__":
    pyppeteer.DEBUG = True
    #cm = ConnectionManager([],["wlo1"])
    #cm = ProxyConnectionManager()
    cm = BasicConnectionManager()
    with cm.start_session() as session:
        if session != None:
            content, title = session.jump_to("https://google.com")
            print(title)
            content, title = session.navigate(session,"a.Fx4vi")
            print(content)
