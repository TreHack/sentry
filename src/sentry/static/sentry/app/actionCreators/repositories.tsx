import * as Sentry from '@sentry/browser';

import RepoActions from 'app/actions/repositoryActions';
import {Client} from 'app/api';
import {Repository} from 'app/types';

type ParamsGet = {
  orgSlug: string;
};

export function getRepositories(api: Client, params: ParamsGet) {
  const {orgSlug} = params;
  const path = `/projects/${orgSlug}/repos/`;

  RepoActions.loadRepo(orgSlug);

  return api
    .requestPromise(path, {
      method: 'GET',
    })
    .then((res: Repository[]) => {
      RepoActions.loadRepoSuccess(orgSlug, res);
    })
    .catch(err => {
      RepoActions.loadRepoError(orgSlug, err);
      Sentry.withScope(scope => {
        scope.setLevel(Sentry.Severity.Warning);
        scope.setFingerprint(['getRepositories-action-creator']);
        Sentry.captureException(err);
      });
    });
}
