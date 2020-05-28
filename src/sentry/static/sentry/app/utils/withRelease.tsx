import React from 'react';
import Reflux from 'reflux';
import createReactClass from 'create-react-class';

import {Client} from 'app/api';
import {Deploy, Release} from 'app/types';
import getDisplayName from 'app/utils/getDisplayName';
import ReleaseStore, {getReleaseStoreKey} from 'app/stores/releaseStore';
import {getRelease, getReleaseDeploys} from 'app/actionCreators/releases';

type InjectedProps = {
  api: Client;
  orgSlug: string;
  projectSlug: string;
  releaseVersion: string;
};

type State = {
  release: Release | undefined;
};

const withRelease = <P extends InjectedProps>(WrappedComponent: React.ComponentType<P>) =>
  createReactClass<Omit<P, keyof InjectedProps> & Partial<InjectedProps>, State>({
    displayName: `withRelease(${getDisplayName(WrappedComponent)})`,
    mixins: [Reflux.listenTo(ReleaseStore, 'onStoreUpdate') as any],

    getInitialState() {
      const {projectSlug, releaseVersion} = this.props as P & InjectedProps;
      const releaseData = ReleaseStore.get(projectSlug, releaseVersion);

      return {release: releaseData.release, deploys: releaseData.deploys};
    },

    componentDidMount() {
      this.fetchRelease();
      this.fetchDeploys();
    },

    fetchRelease() {
      const {api, orgSlug, projectSlug, releaseVersion} = this.props as P & InjectedProps;
      const releaseData = ReleaseStore.get(projectSlug, releaseVersion);

      if (!releaseData.release && !releaseData.releaseLoading) {
        // HACK(leedongwei): Actions fired by the ActionCreators are queued to
        // the back of the event loop, allowing another getRelease for the same
        // release to be fired before the loading state is updated in store.
        // This hack short-circuits that and update the state immediately.
        ReleaseStore.releaseLoading[
          getReleaseStoreKey(projectSlug, releaseVersion)
        ] = true;

        getRelease(api, {orgSlug, projectSlug, releaseVersion});
      }
    },

    fetchDeploys() {
      const {api, orgSlug, projectSlug, releaseVersion} = this.props as P & InjectedProps;
      const releaseData = ReleaseStore.get(projectSlug, releaseVersion);

      if (!releaseData.deploys && !releaseData.deploysLoading) {
        // HACK(leedongwei): Same as above
        ReleaseStore.deploysLoading[
          getReleaseStoreKey(projectSlug, releaseVersion)
        ] = true;

        getReleaseDeploys(api, {orgSlug, projectSlug, releaseVersion});
      }
    },

    onStoreUpdate() {
      const {projectSlug, releaseVersion} = this.props as P & InjectedProps;
      const releaseData = ReleaseStore.get(projectSlug, releaseVersion);

      console.log('onStoreUpdate', releaseData);
      this.setState({release: releaseData.release, deploys: releaseData.deploys});
    },

    render() {
      return (
        <WrappedComponent
          release={this.state.Release as Release}
          deploys={this.state.deploys as Deploy[]}
          {...(this.props as P)}
        />
      );
    },
  });

export default withRelease;
