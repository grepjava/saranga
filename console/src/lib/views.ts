// The fields each listing filters and sorts by, as the broker's views name
// them. They do not change while the broker runs, so they are asked once.

import { api, type Views } from '#lib/api/index.js';

let views: Promise<Views> | undefined;

export function listingViews(): Promise<Views> {
	views ??= api.views().catch((e) => {
		views = undefined;
		throw e;
	});
	return views;
}
