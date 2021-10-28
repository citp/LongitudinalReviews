import pyppeteer.element_handle
from pyppeteer.errors import ElementHandleError


# I believe this is modified from Pyppeteer's source code
# Unfortunately I can't remember why I needed to do this
async def _scrollIntoViewIfNeeded(self) -> None:
    error = await self.executionContext.evaluate('''
        async (element, pageJavascriptEnabled) => {
            if (!element.isConnected)
                return 'Node is detached from document';
            if (element.nodeType !== Node.ELEMENT_NODE)
                return 'Node is not of type HTMLElement';

            $('html, body').animate({
                scrollTop: element.offset().top
            }, 1000);

            // force-scroll if page's javascript is disabled.
            if (!pageJavascriptEnabled) {
                element.scrollIntoView({
                    block: 'center',
                    inline: 'center',
                    behavior: 'instant',
                });
                return false;
            }
            const visibleRatio = await new Promise(resolve => {
                const observer = new IntersectionObserver(entries => {
                    resolve(entries[0].intersectionRatio);
                    observer.disconnect();
                });
                observer.observe(element);
            });
            if (visibleRatio !== 1.0)
                element.scrollIntoView({
                    block: 'center',
                    inline: 'center',
                    behavior: 'instant',
                });
            return false;
        }''', self, self._page._javascriptEnabled)
    if error:
        raise ElementHandleError(error)



pyppeteer.element_handle._scrollIntoViewIfNeeded = _scrollIntoViewIfNeeded
