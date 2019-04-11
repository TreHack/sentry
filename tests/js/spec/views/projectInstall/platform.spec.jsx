import React from 'react';
import {shallow, mount} from 'enzyme';

import {ProjectInstallPlatform} from 'app/views/projectInstall/platform';

describe('ProjectInstallPlatform', function() {
  describe('render()', function() {
    const baseProps = {
      organization: TestStubs.Organization(),
      project: TestStubs.Project(),
      location: {query: {}},
      platformData: {
        platforms: [
          {
            id: 'csharp',
            name: 'C#',
            integrations: [
              {
                id: 'csharp',
                type: 'language',
              },
            ],
          },
          {
            id: 'javascript',
            name: 'JavaScript',
            integrations: [
              {
                id: 'javascript-react',
                type: 'framework',
              },
            ],
          },
          {
            id: 'node',
            name: 'Node.js',
            integrations: [
              {
                id: 'node',
                type: 'language',
              },
              {
                id: 'node-connect',
                type: 'framework',
              },
            ],
          },
        ],
      },
    };

    it('should redirect to if no matching platform', function() {
      const props = {
        ...baseProps,
        params: {
          orgId: baseProps.organization.slug,
          projectId: baseProps.project.slug,
          platform: 'other',
        },
      };

      MockApiClient.addMockResponse({
        url: '/projects/org-slug/project-slug/docs/other/',
        body: {},
      });

      const handleRedirectStub = jest.spyOn(
        ProjectInstallPlatform.prototype,
        'redirectToNeutralDocs'
      );

      // XXX(maxbittker) this is a hack to defeat the method auto binding so we
      // can fully stub the method. It would not be neccessary with es6 class
      // components and it relies on react internals so it's fragile
      const index =
        ProjectInstallPlatform.prototype.__reactAutoBindPairs.indexOf(
          'redirectToNeutralDocs'
        ) + 1;
      ProjectInstallPlatform.prototype.__reactAutoBindPairs[index] = handleRedirectStub;

      mount(<ProjectInstallPlatform {...props} />, {
        organization: {id: '1337'},
      });

      expect(handleRedirectStub).toHaveBeenCalledTimes(1);
    });

    it('should render NotFound if no matching integration/platform', function() {
      const props = {
        ...baseProps,
        params: {
          platform: 'lua',
        },
      };

      const wrapper = shallow(<ProjectInstallPlatform {...props} />, {
        organization: {id: '1337'},
      });

      expect(wrapper.find('NotFound')).toHaveLength(1);
    });

    it('should rendering Loading if integration/platform exists', function() {
      const props = {
        ...baseProps,
        params: {
          platform: 'node-connect',
        },
      };

      const wrapper = shallow(<ProjectInstallPlatform {...props} />, {
        organization: {id: '1337'},
      });

      expect(wrapper.find('LoadingIndicator')).toHaveLength(1);
    });
  });
});
