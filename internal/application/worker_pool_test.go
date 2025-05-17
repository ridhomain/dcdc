package application

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/config"
	// "gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
)

// Mocks used by worker_pool_test.go will be the same as consumer_mocks_test.go
// (mockConfigProvider, mockLogger)

func TestNewWorkerPool_SizingLogic(t *testing.T) {
	numCPU := runtime.GOMAXPROCS(0)

	tests := []struct {
		name                 string
		setupMockConfig      func(mockCfg *mockConfigProvider)
		expectedSize         int
		expectError          bool
		expectedErrorMessage string
	}{
		{
			name: "Absolute override for pool size",
			setupMockConfig: func(mockCfg *mockConfigProvider) {
				mockCfg.On("GetInt", config.KeyWorkers).Return(10).Once()
				mockCfg.On("GetInt", config.KeyWorkersMultiplier).Return(0).Maybe()
				mockCfg.On("GetInt", config.KeyMinWorkers).Return(0).Maybe()
			},
			expectedSize: 10,
			expectError:  false,
		},
		{
			name: "Multiplier used when absolute size is zero or not set",
			setupMockConfig: func(mockCfg *mockConfigProvider) {
				mockCfg.On("GetInt", config.KeyWorkers).Return(0).Once()
				mockCfg.On("GetInt", config.KeyWorkersMultiplier).Return(2).Once()
				mockCfg.On("GetInt", config.KeyMinWorkers).Return(1).Once()
			},
			// If numCPU is small, e.g. 1, then numCPU * 2 = 2, which is > minWorkers (1). Result: 2.
			// If numCPU is large, e.g. 8, then numCPU * 2 = 16, which is > minWorkers (1). Result: 16.
			expectedSize: numCPU * 2,
			expectError:  false,
		},
		{
			name: "Min workers applied if multiplier result is too low",
			setupMockConfig: func(mockCfg *mockConfigProvider) {
				mockCfg.On("GetInt", config.KeyWorkers).Return(0).Once()
				mockCfg.On("GetInt", config.KeyWorkersMultiplier).Return(1).Once()
				mockCfg.On("GetInt", config.KeyMinWorkers).Return(numCPU + 5).Once()
			},
			expectedSize: numCPU + 5,
			expectError:  false,
		},
		{
			name: "Min workers used if multiplier is zero/negative",
			setupMockConfig: func(mockCfg *mockConfigProvider) {
				mockCfg.On("GetInt", config.KeyWorkers).Return(0).Once()
				mockCfg.On("GetInt", config.KeyWorkersMultiplier).Return(0).Once()
				mockCfg.On("GetInt", config.KeyMinWorkers).Return(3).Once()
			},
			expectedSize: 3,
			expectError:  false,
		},
		{
			name: "Default min workers (1) if all configs are zero/invalid and multiplier leads to < 1",
			setupMockConfig: func(mockCfg *mockConfigProvider) {
				mockCfg.On("GetInt", config.KeyWorkers).Return(0).Once()
				mockCfg.On("GetInt", config.KeyWorkersMultiplier).Return(0).Once()
				mockCfg.On("GetInt", config.KeyMinWorkers).Return(0).Once()
			},
			expectedSize: 2,
			expectError:  false,
		},
		{
			name: "Absolute override is negative, should default to min (or 1 if logic caps lower)",
			setupMockConfig: func(mockCfg *mockConfigProvider) {
				mockCfg.On("GetInt", config.KeyWorkers).Return(-5).Once()
				mockCfg.On("GetInt", config.KeyWorkersMultiplier).Return(0).Maybe()
				mockCfg.On("GetInt", config.KeyMinWorkers).Return(0).Maybe()
			},
			expectedSize: 2,
			expectError:  false,
		},
		{
			name: "Multiplier result is 0, min workers is 0, should default to effective minWorkers (2)",
			setupMockConfig: func(mockCfg *mockConfigProvider) {
				mockCfg.On("GetInt", config.KeyWorkers).Return(0).Once()
				mockCfg.On("GetInt", config.KeyWorkersMultiplier).Return(0).Once()
				mockCfg.On("GetInt", config.KeyMinWorkers).Return(0).Once()
			},
			expectedSize: 2,
			expectError:  false,
		},
		{
			name: "Multiplier result is positive but less than min workers (which is also positive)",
			setupMockConfig: func(mockCfg *mockConfigProvider) {
				mockCfg.On("GetInt", config.KeyWorkers).Return(0).Once()
				letMultiplier := 1
				if numCPU == 0 {
					letMultiplier = 0
				}
				mockCfg.On("GetInt", config.KeyWorkersMultiplier).Return(letMultiplier).Once()
				configuredMinWorkers := (numCPU * letMultiplier) + 2
				if configuredMinWorkers <= 0 {
					configuredMinWorkers = 2
				}
				mockCfg.On("GetInt", config.KeyMinWorkers).Return(configuredMinWorkers).Once()
			},
			expectedSize: (numCPU * 1) + 2,
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCfg := new(mockConfigProvider)
			mockLog := new(mockLogger)
			mockLog.On("With", mock.Anything).Return(mockLog)
			mockLog.On("Info", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Maybe()

			tt.setupMockConfig(mockCfg)

			actualPool, err := NewWorkerPool(mockCfg, mockLog)

			if tt.expectError {
				assert.Error(t, err)
				if tt.expectedErrorMessage != "" {
					assert.Contains(t, err.Error(), tt.expectedErrorMessage)
				}
				assert.Nil(t, actualPool)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, actualPool)
				if actualPool != nil {
					if actualPool.pool != nil {
						expected := tt.expectedSize
						if tt.name == "Default min workers (1) if all configs are zero/invalid and multiplier leads to < 1" {
							expected = 2
						}
						if tt.name == "Multiplier result is positive but less than min workers (which is also positive)" {
							calcWorkers := numCPU * 1
							minW := (numCPU * 1) + 2
							if calcWorkers < minW {
								expected = minW
							} else {
								expected = calcWorkers
							}
							if expected <= 0 {
								expected = 2
							}

							if numCPU == 0 {
								expected = 2
							} else {
								expected = numCPU + 2
							}
						}

						assert.Equal(t, expected, actualPool.pool.Cap(), "Pool capacity mismatch for test: "+tt.name)
						actualPool.Release()
					} else {
						t.Errorf("Expected a non-nil antsPool (actualPool.pool) when no error is returned from NewWorkerPool for test: %s", tt.name)
					}
				}
			}
			mockCfg.AssertExpectations(t)
			mockLog.AssertExpectations(t)
		})
	}
}
