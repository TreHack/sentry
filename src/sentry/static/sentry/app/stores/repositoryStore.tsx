import Reflux from 'reflux';

import RepoActions from 'app/actions/releaseActions';
import {Repository} from 'app/types';

type RepositoryStoreInterface = {
  get(
    orgSlug?: string
  ): {
    repos: Array<Repository> | undefined;
    reposLoading: boolean | undefined;
    reposError: Error | undefined;
  };

  orgSlug: string | undefined;
  lastUpdated: number;
  repos: Array<Repository> | undefined;
  reposLoading: boolean | undefined;
  reposError: Error | undefined;

  loadRepos(orgSlug: string): void;
  loadReposSuccess(data: Array<Repository>): void;
  loadReposError(error: Error): void;
};

const RepositoryStoreConfig: Reflux.StoreDefinition & RepositoryStoreInterface = {
  orgSlug: undefined,
  lastUpdated: Date.now(),
  repos: undefined,
  reposLoading: undefined,
  reposError: undefined,

  listenables: [RepoActions],

  init() {
    this.resetRepos();
  },

  resetRepos() {
    this.orgSlug = undefined;
    this.lastUpdated = Date.now();
    this.repos = undefined;
    this.reposLoading = undefined;
    this.reposError = undefined;
  },

  loadRepos(orgSlug: string) {
    this.orgSlug = orgSlug;
    this.lastUpdated = Date.now();
    this.reposLoading = true;
  },

  loadReposError(error: Error) {
    this.lastUpdated = Date.now();
    this.reposLoading = false;
    this.reposError = error;
  },

  loadReposSuccess(data: Array<Repository>) {
    this.lastUpdated = Date.now();
    this.reposLoading = false;
    this.reposError = undefined;
    this.repos = data;
  },

  /**
   * `organizationSlug` is optional. If present, method will run a check if data
   * in the store originated from the same organization
   */
  get(orgSlug?: string) {
    if (orgSlug && orgSlug !== this.orgSlug) {
      return {
        repos: undefined,
        reposLoading: false,
        reposError: new Error('Data in store is outdated'),
      };
    }

    return {
      repos: this.repos,
      reposLoading: this.reposLoading,
      reposError: this.reposError,
    };
  },
};

type RepositoryStore = Reflux.Store & RepositoryStoreInterface;
export default Reflux.createStore(RepositoryStoreConfig) as RepositoryStore;
