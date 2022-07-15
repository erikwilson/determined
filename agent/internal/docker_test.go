package internal

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/docker/distribution/reference"
	"github.com/docker/docker/api/types"

	"github.com/stretchr/testify/require"

	"github.com/determined-ai/determined/master/pkg/actor"
)

func TestGetDockerAuths(t *testing.T) {
	dockerhubAuthConfig := types.AuthConfig{
		Username:      "username",
		Password:      "password",
		ServerAddress: "docker.io",
	}

	exampleDockerConfig := types.AuthConfig{
		Auth:          "token",
		ServerAddress: "https://example.com",
	}

	noServerAuthConfig := types.AuthConfig{
		Username: "username",
		Password: "password",
	}

	dockerAuthSection := map[string]types.AuthConfig{
		"https://index.docker.io/v1/": {
			Auth:          "dockerhubtoken",
			ServerAddress: "docker.io",
		},
		"example.com": {
			Auth:          "exampletoken",
			ServerAddress: "example.com",
		},
	}

	cases := []struct {
		image       string
		expconfReg  *types.AuthConfig
		authConfigs map[string]types.AuthConfig
		expected    types.AuthConfig
	}{
		// No authentication passed in.
		{"detai", nil, nil, types.AuthConfig{}},
		// Correct server passed in for dockerhub.
		{"detai", &dockerhubAuthConfig, nil, dockerhubAuthConfig},
		// Correct server passed in for example.com.
		{"example.com/detai", &exampleDockerConfig, nil, exampleDockerConfig},
		// Different server passed than specified auth.
		{"example.com/detai", &dockerhubAuthConfig, nil, types.AuthConfig{}},
		// No server (behavior is deprecated).
		{"detai", &noServerAuthConfig, nil, noServerAuthConfig},
		{"example.com/detai", &noServerAuthConfig, nil, noServerAuthConfig},

		// Docker auth config gets used.
		{"detai", nil, dockerAuthSection, dockerAuthSection["https://index.docker.io/v1/"]},
		// Expconf takes precedence over docker config.
		{"detai", &dockerhubAuthConfig, dockerAuthSection, dockerhubAuthConfig},
		// We fallback to auths if docker hub has wrong server.
		{
			"example.com/detai", &dockerhubAuthConfig, dockerAuthSection,
			dockerAuthSection["example.com"],
		},
		// We don't return a result if we don't have that serveraddress.
		{"determined.ai/detai", nil, dockerAuthSection, types.AuthConfig{}},
	}

	ctx := getMockDockerActorCtx()
	for _, testCase := range cases {
		d := dockerActor{
			authConfigs: testCase.authConfigs,
		}

		// Parse image to correct format.
		ref, err := reference.ParseNormalizedNamed(testCase.image)
		require.NoError(t, err, "could not get image to correct format")
		ref = reference.TagNameOnly(ref)

		actual, err := d.getDockerAuths(ctx, testCase.expconfReg, ref)
		require.NoError(t, err)
		require.Equal(t, testCase.expected, actual)
	}
}

func getMockDockerActorCtx() *actor.Context {
	var ctx *actor.Context
	sys := actor.NewSystem("")
	child, _ := sys.ActorOf(actor.Addr("child"), actor.ActorFunc(func(context *actor.Context) error {
		ctx = context
		return nil
	}))
	parent, _ := sys.ActorOf(actor.Addr("parent"), actor.ActorFunc(func(context *actor.Context) error {
		context.Ask(child, "").Get()
		return nil
	}))
	sys.Ask(parent, "").Get()
	return ctx
}

func TestRegistryToString(t *testing.T) {
	// No auth just base64ed.
	case1 := types.AuthConfig{
		Email:    "det@example.com",
		Password: "password",
	}
	expected := base64.URLEncoding.EncodeToString(
		[]byte(`{"password":"password","email":"det@example.com"}`))
	actual, err := registryToString(case1)
	require.NoError(t, err, "could not to string auth config")
	require.Equal(t, expected, actual)

	// Auth gets split.
	user, pass := "user", "pass"
	auth := fmt.Sprintf("%s:%s", user, pass)
	case2 := types.AuthConfig{
		Auth: base64.StdEncoding.EncodeToString([]byte(auth)),
	}
	expected = base64.URLEncoding.EncodeToString([]byte(fmt.Sprintf(
		`{"username":"%s","password":"%s"}`, user, pass)))
	actual, err = registryToString(case2)
	require.NoError(t, err, "could not to string auth config")
	require.Equal(t, expected, actual)
}
